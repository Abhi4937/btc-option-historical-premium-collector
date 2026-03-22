"""
IST datetime utilities — foundation module.
All timestamps in the system are IST (UTC+5:30).
Never uses pytz. Uses fixed timedelta offset only.

Verified findings incorporated:
  - Test 1: 24/7 continuous, 1440 candles/day
  - Test 9: expiry ladder logic verified against live exchange
             Wednesday "two Fridays" is correct — 4 unique expiries always
"""

from datetime import datetime, timezone, timedelta, date

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist() -> datetime:
    return datetime.now(IST)


def unix_to_ist(unix_seconds: int) -> datetime:
    return datetime.fromtimestamp(unix_seconds, tz=IST)


def ist_to_unix(dt_ist: datetime) -> int:
    return int(dt_ist.timestamp())


def make_ist(year: int, month: int, day: int,
             hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=IST)


def get_expiry_dt(d: date) -> datetime:
    """Returns 17:30:00 IST datetime for given date."""
    return make_ist(d.year, d.month, d.day, 17, 30)


def _last_friday_of_month(year: int, month: int) -> date:
    """Returns the last Friday of the given month as a date object."""
    from calendar import monthrange
    _, last_day = monthrange(year, month)
    d = date(year, month, last_day)
    days_back = (d.weekday() - 4) % 7   # weekday 4 = Friday
    return d - timedelta(days=days_back)


def get_expiry_ladder(from_dt: datetime) -> list[datetime]:
    """
    Returns list of up to 8 expiry datetimes from given IST datetime.
    Each expiry is at 17:30:00 IST.

    Verified against live exchange on 2026-03-09:
      Live expiries: [Mar 10, 11, 12, 13, 20, 27, Apr 24, May 29]

    Slots:
      1-3: current, next, next-next  (daily)
      4-6: weekly1, weekly2, weekly3 (next 3 Fridays)
      7-8: monthly1, monthly2        (last Friday of next 2 calendar months)
    """
    today_1730 = from_dt.replace(hour=17, minute=30, second=0, microsecond=0)

    current = today_1730 if from_dt < today_1730 else today_1730 + timedelta(days=1)
    nxt      = current + timedelta(days=1)
    nxt_nxt  = current + timedelta(days=2)

    # 1st weekly Friday (skip if nxt_nxt is already Friday)
    days_to_fri = (4 - nxt_nxt.weekday()) % 7
    if nxt_nxt.weekday() == 4:
        days_to_fri = 7
    weekly1 = nxt_nxt + timedelta(days=days_to_fri)
    weekly2 = weekly1 + timedelta(days=7)
    weekly3 = weekly1 + timedelta(days=14)

    # Monthly slots: last Friday of successive calendar months.
    # If weekly3 IS already the last Friday of its month, start from the next month.
    lf_w3_month = _last_friday_of_month(weekly3.year, weekly3.month)
    if weekly3.date() == lf_w3_month:
        m1_year, m1_month = weekly3.year, weekly3.month + 1
    else:
        m1_year, m1_month = weekly3.year, weekly3.month
    if m1_month > 12:
        m1_year += 1
        m1_month -= 12
    lf1 = _last_friday_of_month(m1_year, m1_month)

    m2_year, m2_month = m1_year, m1_month + 1
    if m2_month > 12:
        m2_year += 1
        m2_month -= 12
    lf2 = _last_friday_of_month(m2_year, m2_month)

    monthly1 = make_ist(lf1.year, lf1.month, lf1.day, 17, 30)
    monthly2 = make_ist(lf2.year, lf2.month, lf2.day, 17, 30)

    # Deduplicate by date (some slots may coincide, e.g. weekly3 == monthly)
    seen: set[date] = set()
    ladder: list[datetime] = []
    for dt in [current, nxt, nxt_nxt, weekly1, weekly2, weekly3, monthly1, monthly2]:
        if dt.date() not in seen:
            seen.add(dt.date())
            ladder.append(dt)
    return ladder


def first_appearance(expiry_dt: datetime) -> datetime:
    """
    Returns the earliest IST datetime (00:00) on which `expiry_dt`
    first appears in the expiry ladder.

    Probe window extended to 70 days to cover monthly2 slot
    (~60 days before settlement for next-to-next monthly expiries).
    """
    probe_start = expiry_dt - timedelta(days=70)
    result = expiry_dt - timedelta(days=2)      # conservative default

    check = probe_start.replace(hour=9, minute=0, second=0, microsecond=0)
    while check.date() < expiry_dt.date():
        ladder = get_expiry_ladder(check)
        if any(e.date() == expiry_dt.date() for e in ladder):
            result = check.replace(hour=0, minute=0, second=0)
            break
        check += timedelta(days=1)

    return result


def is_expired(expiry_dt: datetime, check_dt: datetime | None = None) -> bool:
    if check_dt is None:
        check_dt = now_ist()
    return check_dt >= expiry_dt


def month_start_ist(year: int, month: int) -> datetime:
    return make_ist(year, month, 1, 0, 0)


def month_end_ist(year: int, month: int) -> datetime:
    """Last moment of given month (23:59:59 IST of last day)."""
    if month == 12:
        next_month = make_ist(year + 1, 1, 1)
    else:
        next_month = make_ist(year, month + 1, 1)
    return next_month - timedelta(seconds=1)


def format_ist(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S IST")


def ddmmyy(dt: datetime) -> str:
    """Return date as DDMMYY string — used in option symbol construction."""
    return dt.strftime("%d%m%y")


def all_expiry_dates_in_month(year: int, month: int) -> list[datetime]:
    """
    Returns all expiry datetimes (17:30 IST) whose settlement date
    falls within the given month. BTC options have DAILY expiries.
    """
    from calendar import monthrange
    _, last_day = monthrange(year, month)
    return [
        make_ist(year, month, day, 17, 30)
        for day in range(1, last_day + 1)
    ]


def expiry_month_key(expiry_dt: datetime) -> str:
    """Returns 'YYYY-MM' string for the month an expiry settles in."""
    return expiry_dt.strftime("%Y-%m")
