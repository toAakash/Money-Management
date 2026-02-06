from utils.sql_warehouse import get_conn

def get_dashboard_data():
    conn = get_conn()
    cursor = conn.cursor()

    # 1. Per-account balances
    cursor.execute("""
        SELECT
            account_id,
            account_name,
            balance
        FROM mma.finance.accounts
        WHERE is_active = true
    """)
    accounts = cursor.fetchall()

    # 2. Total balance
    cursor.execute("""
        SELECT SUM(balance) FROM mma.finance.accounts
        WHERE is_active = true
    """)
    total_balance = cursor.fetchone()[0]

    # 3. Amount to pay (borrowed / shared-open debit)
    cursor.execute("""
        SELECT COALESCE(SUM(actual_amount - paid_amount), 0)
        FROM mma.finance.transactions
        WHERE finance_type IN ('borrowed', 'shared-open')
          AND flow = 'debit'
    """)
    amount_to_pay = cursor.fetchone()[0]

    # 4. Amount to receive (lent / shared-open credit)
    cursor.execute("""
        SELECT COALESCE(SUM(paid_amount - actual_amount), 0)
        FROM mma.finance.transactions
        WHERE finance_type IN ('lent', 'shared-open')
          AND flow = 'credit'
    """)
    amount_to_receive = cursor.fetchone()[0]

    # 5. Recent 20 transactions
    cursor.execute("""
        SELECT
            txn_id,
            fin_id,
            bill_date,
            amount,
            flow,
            finance_type,
            description
        FROM mma.finance.transactions
        ORDER BY bill_date DESC, created_ts DESC
        LIMIT 20
    """)
    recent_txns = cursor.fetchall()

    return {
        "accounts": accounts,
        "total_balance": total_balance,
        "amount_to_pay": amount_to_pay,
        "amount_to_receive": amount_to_receive,
        "recent_transactions": recent_txns
    }
