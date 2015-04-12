# -*- coding: utf-8 -*-
from datetime import date, timedelta
from collections import defaultdict

from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

from stats.models import RTBStat
from finance.models import RefTransaction
from utils.wrappers import singleton_script_decorator, cron_stats_decorator


class Command(BaseCommand):
    """
    Расчет реферального дохода для всех нерасчитанных дней.
    Проверка дат от первого зарегистрированного реферала.
    """
    help = 'Creates RefTransactions entities (if not exists) for all dates since min referral_user.date_join.'

    @singleton_script_decorator('ref_transactions')
    @cron_stats_decorator('ref_transactions', cron_max_no_job_minutes=25 * 60, cron_min_no_job_minutes=23 * 60)
    def handle(self, *args, **options):
        verbosity = int(options.get('verbosity', 1))
        if verbosity == 1:
            print "verbosity:", verbosity

        date_to = date.today() - timedelta(days=1)

        referral_users = User.objects.filter(profile__parent__isnull=False).select_related('profile__parent')

        parent_referrals_dict = defaultdict(list)
        for referral in referral_users:
            parent_referrals_dict[referral.profile.parent].append(referral)

        new_reftransactions = []
        for parent, referrals in parent_referrals_dict.iteritems():
            date_from = min(r.date_joined.date() for r in referral_users)

            if verbosity == 1:
                print '\n', '-'*20
                print 'parent', parent.username, parent.pk
                print 'from %s to %s' % (date_from, date_to)
                print 'referrals count', len(referrals)

            days_count = (date_to - date_from).days + 1
            date_list = set(date_from + timedelta(days=num) for num in xrange(days_count))
            exist_dates = set(RefTransaction.objects.filter(acceptor=parent).values_list('date', flat=True))
            dates_to_get = date_list - exist_dates

            if verbosity == 1:
                print 'dates_to_get', len(list(dates_to_get)), list(dates_to_get)

            ref_pks = [r.pk for r in referrals]
            r_stats = (RTBStat.objects.filter(block__site__user_id__in=ref_pks, date__in=dates_to_get)
                       .select_related('rtb__client_income', 'block__site__user_id'))

            for dt in dates_to_get:
                if verbosity == 1:
                    print '>>> dt', dt

                date_rtb_stats = None

                for r in referrals:
                    if dt >= r.date_joined.date():
                        if date_rtb_stats is None:
                            date_rtb_stats = [st for st in r_stats if st.date == dt]

                        income = sum(rtb_stat.user_income for rtb_stat in date_rtb_stats
                                     if rtb_stat.block.site.user_id == r.pk)
                        ref_income = income * RefTransaction.REF_PERCENT

                        # creating even if income == 0 - not to check these zero-dates every time
                        new_reftransactions.append(RefTransaction(acceptor=parent, remitter=r, sum=ref_income, date=dt))
                        if verbosity == 1:
                            print '\treferral %s   -   income %s' % (r.pk, income)

        if new_reftransactions:
            RefTransaction.objects.bulk_create(new_reftransactions)