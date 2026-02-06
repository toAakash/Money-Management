from flask import Flask, request, jsonify, render_template, redirect
from utils.sql_warehouse import get_conn

from services.transaction_service import (
    create_transaction,
    update_transaction,
    delete_transaction
)
from services.dashboard_service import get_dashboard_data

app = Flask(__name__)

# -------------------------
# Health
# -------------------------
@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}



@app.route("/api/transactions", methods=["POST"])
def api_create_transaction():
    payload = request.json
    result = create_transaction(payload)
    return jsonify(result), 201

@app.route("/api/transactions/<txn_id>", methods=["PUT"])
def api_update_transaction(txn_id):
    payload = request.json
    result = update_transaction(txn_id, payload)
    return jsonify(result)

@app.route("/api/transactions/<txn_id>", methods=["DELETE"])
def api_delete_transaction(txn_id):
    result = delete_transaction(txn_id)
    return jsonify(result)

@app.route("/api/dashboard", methods=["GET"])
def api_dashboard():
    data = get_dashboard_data()
    return jsonify(data)


@app.route("/accounts")
def accounts_list():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT account_id, account_name, account_type, balance
        FROM mma.finance.accounts
        ORDER BY account_name
    """)
    accounts = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("accounts/list.html", accounts=accounts)


@app.route("/accounts/add", methods=["GET", "POST"])
def accounts_add():
    if request.method == "POST":
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO mma.finance.accounts
            (account_name, account_type, balance, created_ts, updated_ts)
            VALUES (?, ?, ?, current_timestamp(), current_timestamp())
        """, (
            request.form["account_name"],
            request.form["account_type"],
            request.form.get("balance") or 0
        ))

        conn.commit()
        cur.close()
        conn.close()

        return redirect("/accounts")

    return render_template("accounts/add.html")

@app.route("/transactions/add", methods=["GET", "POST"])
def add_transaction_ui():
    if request.method == "POST":
        payload = request.form.to_dict()
        create_transaction(payload)
        return redirect("/")

    return render_template("add_transaction.html")




