import uuid
from utils.sql_warehouse import get_conn


# -------------------------
# Helper
# -------------------------

def _generate_fin_id(fin_id=None):
    return fin_id or str(uuid.uuid4())


def _apply_balance_delta(cursor, account_id, delta):
    """
    Apply balance delta to accounts table.
    Positive delta => credit
    Negative delta => debit
    """
    cursor.execute(
        """
        UPDATE mma.finance.accounts
        SET balance = balance + %s,
            updated_ts = current_timestamp()
        WHERE account_id = %s
        """,
        (delta, account_id)
    )


def _reverse_existing_transaction(cursor, txn_id):
    """
    Reverse balance impact of an existing transaction.
    Used during update / delete.
    """
    cursor.execute(
        """
        SELECT account_id, amount, flow
        FROM mma.finance.transactions
        WHERE txn_id = %s
        """,
        (txn_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError("Transaction not found")

    account_id, amount, flow = row
    delta = amount if flow == "debit" else -amount
    _apply_balance_delta(cursor, account_id, delta)


# -------------------------
# Create Transaction
# -------------------------

def create_transaction(payload):
    """
    Handles:
    - normal transactions
    - transfer (2 rows, same fin_id)
    """
    conn = get_conn()
    cursor = conn.cursor()

    fin_id = _generate_fin_id(payload.get("fin_id"))
    finance_type = payload["finance_type"]

    try:
        if finance_type == "transfer":
            _create_transfer(cursor, payload, fin_id)
        else:
            _create_single_transaction(cursor, payload, fin_id)

        conn.commit()
        return {"status": "success", "fin_id": fin_id}

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()


def _create_single_transaction(cursor, payload, fin_id):
    amount = payload["amount"]
    flow = payload["flow"]
    account_id = payload["account_id"]

    cursor.execute(
        """
        INSERT INTO mma.finance.transactions (
            fin_id, bill_date, paid_date,
            amount, paid_amount, actual_amount,
            flow, finance_type,
            category_id, subcategory_id,
            account_id, method_id,
            tags, description, reference_id, notes,
            created_ts, updated_ts
        )
        VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            current_timestamp(), current_timestamp()
        )
        """,
        (
            fin_id,
            payload.get("bill_date"),
            payload.get("paid_date"),
            amount,
            payload.get("paid_amount"),
            payload.get("actual_amount"),
            flow,
            payload["finance_type"],
            payload.get("category_id"),
            payload.get("subcategory_id"),
            account_id,
            payload.get("method_id"),
            payload.get("tags"),
            payload.get("description"),
            payload.get("reference_id"),
            payload.get("notes"),
        )
    )

    delta = amount if flow == "credit" else -amount
    _apply_balance_delta(cursor, account_id, delta)


def _create_transfer(cursor, payload, fin_id):
    """
    Transfer = debit from source + credit to target
    """
    amount = payload["amount"]

    source_account = payload["source_account_id"]
    target_account = payload["target_account_id"]

    # Debit
    _create_single_transaction(
        cursor,
        {
            **payload,
            "account_id": source_account,
            "flow": "debit",
        },
        fin_id,
    )

    # Credit
    _create_single_transaction(
        cursor,
        {
            **payload,
            "account_id": target_account,
            "flow": "credit",
        },
        fin_id,
    )


# -------------------------
# Update Transaction
# -------------------------

def update_transaction(txn_id, payload):
    """
    Update requires:
    - reversing old balance
    - applying new delta
    """
    conn = get_conn()
    cursor = conn.cursor()

    try:
        _reverse_existing_transaction(cursor, txn_id)

        cursor.execute(
            """
            UPDATE mma.finance.transactions
            SET
                bill_date = %s,
                paid_date = %s,
                amount = %s,
                paid_amount = %s,
                actual_amount = %s,
                flow = %s,
                finance_type = %s,
                category_id = %s,
                subcategory_id = %s,
                account_id = %s,
                method_id = %s,
                tags = %s,
                description = %s,
                reference_id = %s,
                notes = %s,
                updated_ts = current_timestamp()
            WHERE txn_id = %s
            """,
            (
                payload.get("bill_date"),
                payload.get("paid_date"),
                payload["amount"],
                payload.get("paid_amount"),
                payload.get("actual_amount"),
                payload["flow"],
                payload["finance_type"],
                payload.get("category_id"),
                payload.get("subcategory_id"),
                payload["account_id"],
                payload.get("method_id"),
                payload.get("tags"),
                payload.get("description"),
                payload.get("reference_id"),
                payload.get("notes"),
                txn_id,
            )
        )

        delta = payload["amount"] if payload["flow"] == "credit" else -payload["amount"]
        _apply_balance_delta(cursor, payload["account_id"], delta)

        conn.commit()
        return {"status": "updated", "txn_id": txn_id}

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()


# -------------------------
# Delete Transaction
# -------------------------

def delete_transaction(txn_id):
    conn = get_conn()
    cursor = conn.cursor()

    try:
        _reverse_existing_transaction(cursor, txn_id)

        cursor.execute(
            """
            DELETE FROM mma.finance.transactions
            WHERE txn_id = %s
            """,
            (txn_id,)
        )

        conn.commit()
        return {"status": "deleted", "txn_id": txn_id}

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()
