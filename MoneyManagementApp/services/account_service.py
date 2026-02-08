from utils.sql_warehouse import get_conn


def get_all_accounts():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            account_id,
            account_name,
            account_type,
            balance
        FROM mma.finance.accounts
        ORDER BY account_name
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_account_by_id(account_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            account_id,
            account_name,
            account_type,
            balance
        FROM mma.finance.accounts
        WHERE account_id = ?
    """, (account_id,))

    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def create_account(form):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO mma.finance.accounts
        (account_name, account_type, balance, created_ts, updated_ts)
        VALUES (?, ?, ?, current_timestamp(), current_timestamp())
    """, (
        form["account_name"],
        form["account_type"],
        form.get("balance") or 0
    ))

    conn.commit()
    cur.close()
    conn.close()


def update_account(account_id, form):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE mma.finance.accounts
        SET
            account_name = ?,
            account_type = ?,
            balance = ?,
            updated_ts = current_timestamp()
        WHERE account_id = ?
    """, (
        form["account_name"],
        form["account_type"],
        form.get("balance") or 0,
        account_id
    ))

    conn.commit()
    cur.close()
    conn.close()


def delete_account(account_id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM mma.finance.accounts
        WHERE account_id = ?
    """, (account_id,))

    conn.commit()
    cur.close()
    conn.close()
