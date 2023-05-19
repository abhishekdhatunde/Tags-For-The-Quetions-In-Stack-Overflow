import requests
import time
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pyspark import SparkConf
from pyspark import SparkContext
import csv
import boto3

titles = []
tags = []

# S3 bucket and file information
s3_bucket = 'addurkey'
AWS_KEY_ID = 'addkeyid'
AWS_SECRET = 'addkeypass'

# Create an S3 client
s3 = boto3.client('s3',
                  region_name='addregion',
                  aws_access_key_id=AWS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET)

files = ['databases.csv', 'frameworks.csv', 'languages.csv']

for file in files:
    s3.download_file(
        Filename=file,
        Bucket=s3_bucket,
        Key=file)

for file in files:
    s3.delete_object(
        Bucket=s3_bucket,
        Key=file)


def extract():
    # Set the API endpoint and parameters
    url = 'https://api.stackexchange.com/2.3/questions'
    params = {
        'site': 'stackoverflow',
        'fromdate': int((datetime.utcnow() - timedelta(days=1)).timestamp()),  # set the date to fetch questions from
        'todate': int(datetime.utcnow().timestamp()),  # set the date to fetch questions up to
        # 'pagesize': 100  # number of questions per page
        "key": "KLr8ZGSUupm1NH2q8SsmAQ(("
    }

    # Fetch the questions from the API
    questions = []
    has_more = True
    page = 1
    while has_more:
        params['page'] = page
        response = requests.get(url, params=params)
        data = response.json()
        if 'items' in data:
            questions += data['items']
            has_more = data['has_more']
            page += 1
        else:
            print(f"Error fetching questions from page {page}: {data}")
            backoff_time = int(response.headers.get('X-Backoff', 0))
            if backoff_time:
                print(f"Sleeping for {backoff_time} seconds...")
                time.sleep(backoff_time)
            else:
                break

    # Print the question titles
    for question in questions:
        titles.append(question['title'])
        tags.append(question['tags'])


def transform():
    conf = SparkConf().setMaster('local')
    sc = SparkContext(conf=conf)

    stopwords = '''
    a
    a's
    able
    about
    above
    according
    accordingly
    across
    actually
    after
    afterwards
    again
    against
    ain't
    all
    allow
    allows
    almost
    alone
    along
    already
    also
    although
    always
    am
    among
    amongst
    an
    and
    another
    any
    anybody
    anyhow
    anyone
    anything
    anyway
    anyways
    anywhere
    apart
    appear
    appreciate
    appropriate
    are
    aren't
    around
    as
    aside
    ask
    asking
    associated
    at
    available
    away
    awfully
    be
    became
    because
    become
    becomes
    becoming
    been
    before
    beforehand
    behind
    being
    believe
    below
    beside
    besides
    best
    better
    between
    beyond
    both
    brief
    but
    by
    c'mon
    c's
    came
    can
    can't
    cannot
    cant
    cause
    causes
    certain
    certainly
    changes
    clearly
    co
    com
    come
    comes
    concerning
    consequently
    consider
    considering
    contain
    containing
    contains
    corresponding
    could
    couldn't
    course
    currently
    definitely
    described
    despite
    did
    didn't
    different
    do
    does
    doesn't
    doing
    don't
    done
    down
    downwards
    during
    each
    edu
    eg
    eight
    either
    else
    elsewhere
    enough
    entirely
    especially
    et
    etc
    even
    ever
    every
    everybody
    everyone
    everything
    everywhere
    ex
    exactly
    example
    except
    far
    few
    fifth
    first
    five
    followed
    following
    follows
    for
    former
    formerly
    forth
    four
    from
    further
    furthermore
    get
    gets
    getting
    given
    gives
    go
    goes
    going
    gone
    got
    gotten
    greetings
    had
    hadn't
    happens
    hardly
    has
    hasn't
    have
    haven't
    having
    he
    he's
    hello
    help
    hence
    her
    here
    here's
    hereafter
    hereby
    herein
    hereupon
    hers
    herself
    hi
    him
    himself
    his
    hither
    hopefully
    how
    howbeit
    however
    i'd
    i'll
    i'm
    i've
    ie
    if
    ignored
    immediate
    in
    inasmuch
    inc
    indeed
    indicate
    indicated
    indicates
    inner
    insofar
    instead
    into
    inward
    is
    isn't
    it
    it'd
    it'll
    it's
    its
    itself
    just
    keep
    keeps
    kept
    know
    knows
    known
    last
    lately
    later
    latter
    latterly
    least
    less
    lest
    let
    let's
    like
    liked
    likely
    little
    look
    looking
    looks
    ltd
    mainly
    many
    may
    maybe
    me
    mean
    meanwhile
    merely
    might
    more
    moreover
    most
    mostly
    much
    must
    my
    myself
    name
    namely
    nd
    near
    nearly
    necessary
    need
    needs
    neither
    never
    nevertheless
    new
    next
    nine
    no
    nobody
    non
    none
    noone
    nor
    normally
    not
    nothing
    novel
    now
    nowhere
    obviously
    of
    off
    often
    oh
    ok
    okay
    old
    on
    once
    one
    ones
    only
    onto
    or
    other
    others
    otherwise
    ought
    our
    ours
    ourselves
    out
    outside
    over
    overall
    own
    particular
    particularly
    per
    perhaps
    placed
    please
    plus
    possible
    presumably
    probably
    provides
    que
    quite
    qv
    rather
    rd
    re
    really
    reasonably
    regarding
    regardless
    regards
    relatively
    respectively
    right
    said
    same
    saw
    say
    saying
    says
    second
    secondly
    see
    seeing
    seem
    seemed
    seeming
    seems
    seen
    self
    selves
    sensible
    sent
    serious
    seriously
    seven
    several
    shall
    she
    should
    shouldn't
    since
    six
    so
    some
    somebody
    somehow
    someone
    something
    sometime
    sometimes
    somewhat
    somewhere
    soon
    sorry
    specified
    specify
    specifying
    still
    sub
    such
    sup
    sure
    t's
    take
    taken
    tell
    tends
    th
    than
    thank
    thanks
    thanx
    that
    that's
    thats
    the
    their
    theirs
    them
    themselves
    then
    thence
    there
    there's
    thereafter
    thereby
    therefore
    therein
    theres
    thereupon
    these
    they
    they'd
    they'll
    they're
    they've
    think
    third
    this
    thorough
    thoroughly
    those
    though
    three
    through
    throughout
    thru
    thus
    to
    together
    too
    took
    toward
    towards
    tried
    tries
    truly
    try
    trying
    twice
    two
    un
    under
    unfortunately
    unless
    unlikely
    until
    unto
    up
    upon
    us
    use
    used
    useful
    uses
    using
    usually
    value
    various
    very
    via
    viz
    vs
    want
    wants
    was
    wasn't
    way
    we
    we'd
    we'll
    we're
    we've
    welcome
    well
    went
    were
    weren't
    what
    what's
    whatever
    when
    whence
    whenever
    where
    where's
    whereafter
    whereas
    whereby
    wherein
    whereupon
    wherever
    whether
    which
    while
    whither
    who
    who's
    whoever
    whole
    whom
    whose
    why
    will
    willing
    wish
    with
    within
    without
    won't
    wonder
    would
    would
    wouldn't
    yes
    yet
    you
    you'd
    you'll
    you're
    you've
    your
    yours
    yourself
    yourselves
    zero
    '''

    stopwords = stopwords.splitlines()

    def stopwords_remover(word):
        if word not in stopwords:
            return True

    def re_splitter(line):
        return re.split('[^a-z]', line)

    results = sc.parallelize(titles) \
        .map(str.lower) \
        .flatMap(re_splitter) \
        .filter(stopwords_remover) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, x: a + x) \
        .sortBy(lambda tup: tup[1], ascending=False) \
        .collect()

    pairs = {result[0]: result[1] for result in results}

    databases = ['MySQL',
                 'PostgreSQL',
                 'SQLite',
                 'MongoDB',
                 'MariaDB',
                 'Oracle',
                 'Firebase',
                 'Redis',
                 'Elasticsearch',
                 'DynamoDB',
                 'Cassandra']

    tags_all = []
    for tag in tags:
        for single in tag:
            tags_all.append(single)

    results_tags = sc.parallelize(tags_all) \
        .map(str.lower) \
        .flatMap(re_splitter) \
        .filter(stopwords_remover) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, x: a + x) \
        .sortBy(lambda tup: tup[1], ascending=False) \
        .collect()

    pairs2 = {result[0]: result[1] for result in results_tags}

    frameworks = ['ASP',
                  'Angular',
                  'Django',
                  'Express',
                  'Flask',
                  'Laravel',
                  'React',
                  'Ruby',
                  'Spring',
                  'Symfony',
                  'Vue',
                  'jQuery']

    languages = ['Assembly',
                 'Bash',
                 'Shell',
                 'C',
                 'Dart',
                 'HTML',
                 'CSS',
                 'Haskell',
                 'Java',
                 'JavaScript',
                 'Julia',
                 'Kotlin',
                 'PHP',
                 'Perl',
                 'Python',
                 'R',
                 'Ruby',
                 'Rust',
                 'SQL',
                 'Scala',
                 'Swift',
                 'TypeScript',
                 'VBA']

    di_lang = dict()

    for language in languages:
        language = language.lower()
        di_lang[language] = pairs2.get(language)

    languages_df = pd.DataFrame.from_dict(di_lang, orient='index').transpose()
    languages_df.index.name = 'Date'
    languages_df = languages_df.rename(index={0: str(datetime.now().date())})
    languages_df = languages_df.replace(np.nan, '', regex=True)
    date_col = str(datetime.now().date())
    languages_df.insert(0, 'Date', date_col)
    languages_first_row = list(languages_df.iloc[0, :])

    # define the file path
    languages_file_path = 'languages.csv'

    # open the file in append mode
    with open(languages_file_path, mode='a', newline='') as languages_csv_file:
        # create a CSV writer object
        writer = csv.writer(languages_csv_file)

        # add the new data to the file
        writer.writerow(languages_first_row)

    di_database = dict()

    for database in databases:
        database = database.lower()
        di_database[database] = pairs2.get(database)

    databases_df = pd.DataFrame.from_dict(di_database, orient='index').transpose()
    databases_df.index.name = 'Date'
    databases_df = databases_df.rename(index={0: str(datetime.now().date())})

    databases_df = databases_df.replace(np.nan, '', regex=True)
    databases_df.insert(0, 'Date', date_col)

    databases_first_row = list(databases_df.iloc[0, :])

    # define the file path
    databases_file_path = 'databases.csv'

    # open the file in append mode
    with open(databases_file_path, mode='a', newline='') as databases_csv_file:
        # create a CSV writer object
        writer = csv.writer(databases_csv_file)

        # add the new data to the file
        writer.writerow(databases_first_row)

    di_framework = dict()

    for framework in frameworks:
        framework = framework.lower()
        di_framework[framework] = pairs2.get(framework)

    framework_df = pd.DataFrame.from_dict(di_framework, orient='index').transpose()
    framework_df.index.name = 'Date'
    framework_df = framework_df.rename(index={0: str(datetime.now().date())})
    framework_df = framework_df.replace(np.nan, '', regex=True)
    framework_df.insert(0, 'Date', date_col)

    frameworks_first_row = list(framework_df.iloc[0, :])

    frameworks_file_path = 'frameworks.csv'

    # open the file in append mode
    with open(frameworks_file_path, mode='a', newline='') as frameworks_csv_file:
        # create a CSV writer object
        writer = csv.writer(frameworks_csv_file)

        # add the new data to the file
        writer.writerow(frameworks_first_row)


extract()
transform()

for file in files:
    s3.upload_file(
        Filename=file,
        Bucket=s3_bucket,
        Key=file)

