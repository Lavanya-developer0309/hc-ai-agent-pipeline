"""
Run 00_bootstrap_new_account.sql in the NEW Snowflake account as your admin user.

    cd ~/Desktop/"Lavanya AI"/ai-data-profiling-agent
    .venv/bin/python ../hc-pipeline/snowflake/run_bootstrap.py

Asks for your username, password and MFA code here in the Terminal.
Nothing is saved or printed.
"""
import getpass
import os
from pathlib import Path

import snowflake.connector

SQL = Path(__file__).with_name("00_bootstrap_new_account.sql")


def main():
    account = os.environ.get("SNOWFLAKE_ACCOUNT") or input("Snowflake account (orgname-accountname): ").strip()
    print(f"Snowflake account: {account}\n")
    user = input("Your Snowflake username (the admin you created at sign-up): ").strip()
    password = getpass.getpass("Password (hidden while typing): ")
    passcode = input("MFA code from your authenticator app (6 digits) - or press Enter if you use Duo push: ").strip()

    params = dict(account=account, user=user, password=password, authenticator="username_password_mfa",
                  role="ACCOUNTADMIN", client_request_mfa_token=False, login_timeout=120)
    if passcode:
        params["passcode"] = passcode
    print("\nSigning in..." + ("" if passcode else " (approve the push on your phone)"))
    try:
        conn = snowflake.connector.connect(**params)
    except Exception as e:
        print("\nSign-in failed:", str(e).splitlines()[0][:300])
        print("If you use a passkey or Windows Hello for MFA, this program can't use it -"
              " run the SQL file in Snowsight instead.")
        return
    try:
        cursors = conn.execute_string(SQL.read_text(), remove_comments=True)
        last = cursors[-1].fetchone()
        print(f"\n✅ {len(cursors)} statements run. {last[0]}  (account {last[1]}, region {last[2]})")
        print("Go back to Claude and say: done")
    except Exception as e:
        print("\n❌ Setup stopped with an error:", str(e).splitlines()[0][:300])
        print("Copy this error line to Claude (it contains no secrets).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
