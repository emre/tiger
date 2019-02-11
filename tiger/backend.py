from lightsteem.helpers.amount import Amount

from .utils import get_db_connection, get_lightsteem_client


def upsert(table, delegation):
    """ Inserts or updates the delegation information to the database.

    If there is already a delegation registered from delegator to delegatee,
    it updates the row instead of creating a new one.

    :param table: table object of Dataset
    :param delegation: Delegation row from the steemd
    :return: None
    """
    query = dict(
        delegator=delegation["delegator"],
        delegatee=delegation["delegatee"],
    )

    amount_in_vests = float(Amount.from_asset(
        delegation["vesting_shares"]).amount)

    delegation_object = dict(
        delegator=delegation["delegator"],
        delegatee=delegation["delegatee"],
        vests=amount_in_vests,
        created_at=delegation["min_delegation_time"]
    )

    if table.find_one(**query):
        table.update(delegation_object, ['delegator', 'delegatee'])
    else:
        table.insert(delegation_object)


def fetch_active_delegations(database_api, start,
                             limit, order="by_delegation"):
    """By using `database_api.list_vesting_delegations` it fetches all
    active delegations in the STEEM network and registers to the database.

    :param database_api: A Lightsteem client instance with the `database_api`
        namespace.
    :param start (list): A delegator:delegatee list to start fetching
    :param limit (int): Number of rows per page
    :param order: "by_delegation"
    :return: None
    """
    result_list = []
    delegations = database_api.list_vesting_delegations({
        "start": start,
        "limit": limit,
        "order": order,
    })
    db = get_db_connection()
    table = db['delegations']
    while len(delegations["delegations"]) > 1:
        db.begin()
        last_delegator = delegations["delegations"][-1]["delegator"]
        last_delegatee = delegations["delegations"][-1]["delegatee"]
        print(f"Getting {last_delegator}:{last_delegatee}")
        delegations = database_api.list_vesting_delegations({
            "start": [last_delegator, last_delegatee],
            "limit": limit,
            "order": order,
        })
        for delegation in delegations.get("delegations")[1:]:
            upsert(table, delegation)
        db.commit()

    print(result_list)


def run_fetcher():
    database_api = get_lightsteem_client()('database_api')
    delegations = fetch_active_delegations(
        database_api,
        ["", ""],
        1000
    )
