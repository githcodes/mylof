import os
import time
import random
import psycopg2
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

# ----- 读取账号凭证 -----
USER1 = os.environ.get('JISILU_USER1')
PASSWORD1 = os.environ.get('JISILU_PASSWORD1')
USER2 = os.environ.get('JISILU_USER2')
PASSWORD2 = os.environ.get('JISILU_PASSWORD2')
DATABASE_URL = os.environ.get('DATABASE_URL')

ACCOUNTS = [
    {'user': USER1, 'pass': PASSWORD1},
    {'user': USER2, 'pass': PASSWORD2},
]

def login_and_get_cookies(user, password):
    """登录并返回 Cookie 字典，失败返回 None"""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
    })

    try:
        print(f"🔄 正在使用账号 {user[:3]}*** 登录...")
        driver.get('https://www.jisilu.cn/account/login/')
        wait = WebDriverWait(driver, 30)

        user_input = wait.until(EC.visibility_of_element_located((By.NAME, 'user_name')))
        user_input.clear()
        user_input.send_keys(user)
        pass_input = driver.find_element(By.NAME, 'password')
        pass_input.clear()
        pass_input.send_keys(password)
        pass_input.send_keys(Keys.TAB)

        # 勾选两个复选框
        driver.execute_script("""
            var cbs = document.querySelectorAll('input[type="checkbox"]');
            if(cbs.length >= 2) {
                cbs[0].checked = true;
                cbs[1].checked = true;
                cbs[0].dispatchEvent(new Event('change', {bubbles: true}));
                cbs[1].dispatchEvent(new Event('change', {bubbles: true}));
            }
        """)

        # 点击登录
        login_btn = driver.execute_script("""
            var elements = document.querySelectorAll('a, button, input[type="submit"]');
            for (var i=0; i<elements.length; i++) {
                var el = elements[i];
                var text = el.textContent || el.value || '';
                if (text.indexOf('登录') !== -1) return el;
            }
            return null;
        """)
        if login_btn:
            driver.execute_script("arguments[0].click();", login_btn)
        else:
            raise Exception("未找到登录按钮")

        WebDriverWait(driver, 30).until(lambda d: 'login' not in d.current_url.lower())
        print(f"✅ 登录成功: {driver.current_url}")

        all_cookies = driver.get_cookies()
        target_cookies = {}
        for c in all_cookies:
            if c['name'] in ['kbzw__Session', 'kbzw__user_login']:
                target_cookies[c['name']] = c['value']
        if 'kbzw__Session' in target_cookies and 'kbzw__user_login' in target_cookies:
            return target_cookies
        else:
            return None
    except Exception as e:
        print(f"❌ 登录失败 ({user[:3]}***): {e}")
        return None
    finally:
        driver.quit()

def update_cookies_in_db(cookies_dict):
    if not cookies_dict:
        return False
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        for key, value in cookies_dict.items():
            cur.execute("""
                INSERT INTO cookies (cookie_key, cookie_value, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (cookie_key)
                DO UPDATE SET cookie_value = EXCLUDED.cookie_value, updated_at = CURRENT_TIMESTAMP;
            """, (key, value))
        conn.commit()
        print("✅ Cookie 已更新到数据库")
        return True
    except Exception as e:
        print(f"❌ 数据库更新失败: {e}")
        return False
    finally:
        if cur: cur.close()
        if conn: conn.close()

def main():
    if not all([USER1, PASSWORD1, USER2, PASSWORD2, DATABASE_URL]):
        print("❌ 请确保所有 Secrets 已设置")
        exit(1)

    # 随机选择首选账号
    first = random.randint(0, 1)
    order = [first, 1 - first]
    for idx in order:
        account = ACCOUNTS[idx]
        if not account['user'] or not account['pass']:
            continue
        cookies = login_and_get_cookies(account['user'], account['pass'])
        if cookies:
            if update_cookies_in_db(cookies):
                print(f"🎉 更新成功 (账号 {account['user'][:3]}***)")
                return
        else:
            print(f"⚠️ 账号 {account['user'][:3]}*** 失败，尝试下一个")

    print("❌ 所有账号均失败")
    exit(1)

if __name__ == '__main__':
    main()