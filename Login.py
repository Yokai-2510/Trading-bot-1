# Login.py
from urllib.parse import quote, urlparse, parse_qs
from playwright.sync_api import Playwright, sync_playwright
from credentials import API_KEY, SECRET_KEY, RURL, TOTP_KEY, MOBILE_NO, PIN
import requests
import pyotp
import json

access_token = None  # Global variable to store the access token

def get_access_token():
    global access_token
    if access_token:
        return access_token

    def login_auto(api_key, rurl, mobile_no, pin, totp_key):
        auth_url = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={quote(rurl, safe="")}'

        with sync_playwright() as playwright:
            browser = playwright.firefox.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            with page.expect_request(f"*{rurl}?code*") as request:
                page.goto(auth_url)
                page.locator("#mobileNum").click()
                page.locator("#mobileNum").fill(mobile_no)
                page.get_by_role("button", name="Get OTP").click()

                page.locator("#otpNum").click()
                otp = pyotp.TOTP(totp_key).now()
                page.locator("#otpNum").fill(otp)
                page.get_by_role("button", name="Continue").click()

                page.get_by_label("Enter 6-digit PIN").click()
                page.get_by_label("Enter 6-digit PIN").fill(pin)
                page.get_by_role("button", name="Continue").click()

                page.wait_for_load_state()

            url = request.value.url
            parsed = urlparse(url)
            code = parse_qs(parsed.query)['code'][0]

            context.close()
            browser.close()
            return code

    # Begin the process
    print("Please wait for 30 seconds to connect to upstox Account")
    auth_code = login_auto(API_KEY, RURL, MOBILE_NO, PIN, TOTP_KEY)

    # Retrieve access token
    url = 'https://api-v2.upstox.com/login/authorization/token'
    headers = {
        'accept': 'application/json',
        'Api-Version': '2.0',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'code': auth_code,
        'client_id': API_KEY,
        'client_secret': SECRET_KEY,
        'redirect_uri': RURL,
        'grant_type': 'authorization_code'
    }
    response = requests.post(url, headers=headers, data=data)
    json_response = response.json()
    access_token = json_response['access_token']

    with open("access_token.txt", "w") as file:
        file.write(access_token)

    return access_token

if __name__ == "__main__":
    # If the script is executed directly, call the get_access_token function
    get_access_token()
    print(access_token)
