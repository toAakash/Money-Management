from flask import Flask, request, jsonify, render_template, redirect
from utils.sql_warehouse import get_conn
from services.account_service import(
    get_all_accounts,create_account
)

app = Flask(__name__)

# -------------------------
# Health
# -------------------------
@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}

@app.route("/accounts")
def accounts_list():
    accounts = get_all_accounts()
    return render_template("accounts/list.html", accounts=accounts)


@app.route("/accounts/add", methods=["GET", "POST"])
def accounts_add():
    if request.method == "POST":
        create_account(request.form)
        return redirect("/accounts")

    return render_template("accounts/add.html")
