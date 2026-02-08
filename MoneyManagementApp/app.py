from flask import Flask, request, render_template, redirect

from services.account_service import (
    get_all_accounts,
    get_account_by_id,
    create_account,
    update_account,
    delete_account
)

app = Flask(__name__)



# ----------------------
# ACCOUNTS - LIST
# ----------------------
@app.route("/accounts")
def accounts_list():
    accounts = get_all_accounts()
    return render_template("accounts/list.html", accounts=accounts)


# ----------------------
# ACCOUNTS - ADD
# ----------------------
@app.route("/accounts/add", methods=["GET", "POST"])
def accounts_add():
    if request.method == "POST":
        create_account(request.form)
        return redirect("/accounts")

    return render_template("accounts/add.html")


# ----------------------
# ACCOUNTS - EDIT / DELETE
# ----------------------
@app.route("/accounts/<int:account_id>/edit", methods=["GET", "POST"])
def accounts_edit(account_id):

    if request.method == "POST":

        # DELETE action
        if request.form.get("_action") == "delete":
            delete_account(account_id)
            return redirect("/accounts")

        # SAVE action
        update_account(account_id, request.form)
        return redirect("/accounts")

    account = get_account_by_id(account_id)
    return render_template("accounts/edit.html", account=account)


# ----------------------
# HEALTH (optional)
# ----------------------
@app.route("/health")
def health():
    return {"status": "ok"}
