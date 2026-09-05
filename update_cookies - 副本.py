import os
import time
import psycopg2
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

# ----- 从环境变量读取敏感信息 -----
USER = os.environ.get('JISILU_USER')
PASSWORD = os.environ.get('JISILU_PASSWORD')
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_cookies():
    """使用 Selenium 登录集思录，返回包含 kbzw__Session 和 kbzw__user_login 的字典"""
    options = webdriver.ChromeOptions()
    # GitHub Actions 必须使用无头模式
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
        print("🔄 正在启动浏览器并登录...")
        driver.get('https://www.jisilu.cn/account/login/')
        wait = WebDriverWait(driver, 30)

        # 输入账号密码
        user_input = wait.until(EC.visibility_of_element_located((By.NAME, 'user_name')))
        user_input.clear()
        user_input.send_keys(USER)
        pass_input = driver.find_element(By.NAME, 'password')
        pass_input.clear()
        pass_input.send_keys(PASSWORD)
        pass_input.send_keys(Keys.TAB)

        # 勾选两个复选框（使用 JS 一次性搞定）
        driver.execute_script("""
            var cbs = document.querySelectorAll('input[type="checkbox"]');
            if(cbs.length >= 2) {
                cbs[0].checked = true;
                cbs[1].checked = true;
                cbs[0].dispatchEvent(new Event('change', {bubbles: true}));
                cbs[1].dispatchEvent(new Event('change', {bubbles: true}));
            }
        """)
        print("✅ 已勾选两个复选框")

        # 查找并点击登录按钮（万能查找）
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
            print("✅ 已点击登录按钮")
        else:
            raise Exception("未找到登录按钮")

                # 点击登录后，等待跳转到首页（通过 URL 判断或元素判断）
        try:
            # 等待 URL 变为根路径
            WebDriverWait(driver, 30).until(lambda d: d.current_url == 'https://www.jisilu.cn/')
            print(f"✅ 登录成功，当前 URL: {driver.current_url}")
        except:
            current_url = driver.current_url
            if 'login' in current_url.lower():
                print(f"❌ 登录失败，当前 URL 仍为登录页: {current_url}")
                # 保存截图和页面源码以便调试
                driver.save_screenshot('login_failed.png')
                with open('page_source.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                raise Exception("登录失败，未能跳转")
            else:
                print(f"✅ 登录成功，当前 URL: {current_url}")

        # 打印所有 Cookie 名称，确认是否包含 user_login
        all_cookies = driver.get_cookies()
        print("所有 Cookie 名称:", [c['name'] for c in all_cookies])
        target_cookies = {}
        for c in all_cookies:
            if c['name'] in ['kbzw__Session', 'kbzw__user_login']:
                target_cookies[c['name']] = c['value']
        print(f"目标 Cookie: {list(target_cookies.keys())}")
        return target_cookies

        # 提取需要的 Cookie
        all_cookies = driver.get_cookies()
        target_cookies = {}
        for c in all_cookies:
            if c['name'] in ['kbzw__Session', 'kbzw__user_login']:
                target_cookies[c['name']] = c['value']
        print(f"✅ 获取到 Cookie: {list(target_cookies.keys())}")
        return target_cookies

    except Exception as e:
        print(f"❌ 登录失败: {e}")
        # 可选：保存截图用于调试
        # driver.save_screenshot('error.png')
        return None
    finally:
        driver.quit()
        print("🚪 浏览器已关闭")

def update_cookies_in_db(cookies_dict):
    """将 Cookie 更新到 PostgreSQL 数据库的 cookies 表"""
    if not cookies_dict:
        print("❌ 没有 Cookie 需要更新")
        return

    try:
        print("📡 正在连接数据库...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        for key, value in cookies_dict.items():
            cur.execute("""
                INSERT INTO cookies (cookie_key, cookie_value, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (cookie_key)
                DO UPDATE SET cookie_value = EXCLUDED.cookie_value, updated_at = CURRENT_TIMESTAMP;
            """, (key, value))
            print(f"✅ Cookie '{key}' 已更新")

        conn.commit()
        print("✅ 所有 Cookie 已成功提交到数据库")
    except Exception as e:
        print(f"❌ 数据库操作失败: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("📪 数据库连接已关闭")

if __name__ == '__main__':
    # 检查环境变量是否齐全
    if not all([USER, PASSWORD, DATABASE_URL]):
        print("❌ 错误：请确保 JISILU_USER, JISILU_PASSWORD, DATABASE_URL 环境变量已设置")
        exit(1)

    cookies = get_cookies()
    if cookies:
        update_cookies_in_db(cookies)
        print("🎉 更新任务成功完成")
    else:
        print("❌ 未能获取到 Cookie，任务失败")
        exit(1)