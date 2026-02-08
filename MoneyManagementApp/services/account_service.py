from utils.sql_warehouse import get_connection


def get_all_accounts():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT account_id, account_name, account_type, balance
        FROM mma.finance.accounts
        ORDER BY account_name
    """)
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


def create_account(data):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO mma.finance.accounts
        (account_name, account_type, balance, created_ts, updated_ts)
        VALUES (?, ?, ?, current_timestamp(), current_timestamp())
    """, (
        data["account_name"],
        data["account_type"],
        data.get("balance") or 0
    ))

    conn.commit()
    cur.close()
    conn.close()
