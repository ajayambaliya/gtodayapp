import io
import os
import requests
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime
from deep_translator import GoogleTranslator
import asyncio
import logging
import tempfile
from PIL import Image
import ftplib
from urllib.parse import urlparse
import hashlib
import traceback
import sys
import random
import json
import firebase_admin
from firebase_admin import credentials, messaging
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pymongo import MongoClient  # Added for MongoDB integration

# Load environment variables
load_dotenv()

# Configure logging to log only to console
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# MongoDB Configuration
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLLECTION = 'scraped_urls'

# Database configuration (MySQL)
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# FTP Configuration
FTP_CONFIG = {
    'host': os.getenv('FTP_HOST'),
    'user': os.getenv('FTP_USER'),
    'password': os.getenv('FTP_PASSWORD'),
    'port': 21,
    'upload_path': '/'
}

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL = os.getenv('TELEGRAM_CHANNEL')

# Initialize Firebase
def initialize_firebase():
    if not firebase_admin._apps:
        service_account_json = os.getenv('FIREBASE_SERVICE_ACCOUNT')
        service_account_path = os.getenv('FIREBASE_SERVICE_ACCOUNT_PATH')
        logging.debug(f"Firebase service account path: {service_account_path}")
        if service_account_json:
            cred = credentials.Certificate(json.loads(service_account_json))
        elif service_account_path and os.path.exists(service_account_path):
            cred = credentials.Certificate(service_account_path)
        else:
            raise ValueError("No valid Firebase credentials provided")
        firebase_admin.initialize_app(cred)
        logging.info("Firebase initialized successfully")
    else:
        logging.debug("Firebase already initialized, skipping reinitialization")

initialize_firebase()

CATEGORY_MAP = {
    "વિજ્ઞાન અને ટેકનોલોજી વર્તમાન બાબતો": 12,
    "સંરક્ષણ વર્તમાન બાબતો": 13,
    "કાનૂની અને બંધારણ વર્તમાન બાબતો": 14,
    "પર્યાવરણ વર્તમાન બાબતો": 15,
    "સરકારી યોજનાઓ વર્તમાન બાબતો": 16,
    "અર્થતંત્ર અને બેંકિંગ વર્તમાન બાબતો": 17,
    "આંતરરાષ્ટ્રીય / વિશ્વ વર્તમાન બાબતો": 18,
    "સમિટ અને પરિષદો": 19,
    "મહત્વપૂર્ણ દિવસો અને ઘટનાઓ વર્તમાન બાબતો": 20,
    "અહેવાલો અને સૂચકાંકો વર્તમાન બાબતો": 21,
    "રમતગમત વર્તમાન બાબતો": 22,
    "સમાચારમાં પુરસ્કારો, સન્માનો અને વ્યક્તિઓ": 23,
    "કૃષિ વર્તમાન બાબતો": 24,
    "કલા અને સંસ્કૃતિ વર્તમાન બાબતો": 25,
    "વિજ્ and ાન અને તકનીકી વર્તમાન બાબતો":12
}

# MongoDB connection
def create_mongo_connection():
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        logging.info("MongoDB connection successful")
        return client, collection
    except Exception as e:
        logging.error(f"MongoDB connection error: {e}")
        return None, None

def log_scraped_url(collection, url):
    try:
        document = {
            'url': url,
            'scraped_at': datetime.utcnow()
        }
        collection.insert_one(document)
        logging.debug(f"Logged URL to MongoDB: {url}")
    except Exception as e:
        logging.error(f"Error logging URL to MongoDB: {e}")

def is_url_scraped(collection, url):
    try:
        result = collection.find_one({'url': url})
        return result is not None
    except Exception as e:
        logging.error(f"Error checking URL in MongoDB: {e}")
        return False

def create_db_connection():
    try:
        logging.info("Attempting to create database connection...")
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            port=3306,
            use_pure=True,
            connection_timeout=10
        )
        connection.ping(reconnect=True)
        logging.info("Database connection successful")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
        return None

def check_and_reconnect(connection):
    try:
        if connection and connection.is_connected():
            connection.ping(reconnect=True)
            logging.debug("Database connection is alive")
            return connection
        logging.info("Connection lost, reconnecting...")
        return create_db_connection()
    except mysql.connector.Error as err:
        logging.error(f"Error during connection check: {err}")
        return create_db_connection()

def fetch_article_urls(base_url, pages):
    article_urls = []
    session = requests.Session()
    logging.info(f"Fetching article URLs from {base_url} for {pages} pages")
    for page in range(1, pages + 3):
        url = base_url if page == 1 else f"{base_url}page/{page}/"
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            page_articles = [a_tag['href'] for h1_tag in soup.find_all('h1', id='list')
                           if (a_tag := h1_tag.find('a')) and a_tag.get('href')]
            article_urls.extend(page_articles)
            logging.info(f"Found {len(page_articles)} articles on page {page}")
        except requests.RequestException as e:
            logging.error(f"Error fetching page {page}: {e}")
            continue
    return article_urls

translation_cache = {}

def translate_to_gujarati(text):
    if not text or len(text.strip()) == 0:
        logging.debug("Empty text received for translation, returning as-is")
        return text
    if text in translation_cache:
        logging.debug(f"Using cached translation for: {text[:50]}...")
        return translation_cache[text]
    try:
        logging.debug(f"Translating: {text[:50]}...")
        translator = GoogleTranslator(source='auto', target='gu')
        translated = translator.translate(text)
        translation_cache[text] = translated
        logging.debug(f"Translated to: {translated[:50]}...")
        return translated
    except Exception as e:
        logging.warning(f"Translation error: {e}, returning original text")
        return text

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type((requests.RequestException, ftplib.all_errors)))
def download_and_process_image(image_url):
    temp_file_path = None
    ftp = None
    try:
        logging.info(f"Downloading image from: {image_url}")
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp_file:
            temp_file_path = tmp_file.name
            logging.debug(f"Created temp file: {temp_file_path}")
            img = Image.open(io.BytesIO(response.content)).convert('RGB')
            img.save(temp_file_path, 'PNG')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            hash_object = hashlib.md5(image_url.encode())
            filename = f"news_{timestamp}_{hash_object.hexdigest()[:8]}.png"
            ftp = ftplib.FTP()
            ftp.connect(FTP_CONFIG['host'], FTP_CONFIG['port'])
            ftp.login(FTP_CONFIG['user'], FTP_CONFIG['password'])
            ftp.cwd(FTP_CONFIG['upload_path'])
            with open(temp_file_path, 'rb') as file:
                ftp.storbinary(f'STOR {filename}', file)
            logging.info(f"Image uploaded as: {filename}")
            return filename
    except Exception as e:
        logging.error(f"Image processing error: {e}")
        raise
    finally:
        if ftp and ftp.sock:
            ftp.quit()
            logging.debug("FTP connection closed")
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            logging.debug(f"Temporary file deleted: {temp_file_path}")

def format_content_as_html(content_list):
    try:
        translated_content = next((item['text'] for item in content_list if item.get('type') in ['heading', 'paragraph'] and item.get('text')), "Article")
        translated_first_paragraph = next((item['text'] for item in content_list if item.get('type') == 'paragraph' and item.get('text')), "")
        logging.debug(f"Formatting HTML with title: {translated_content[:50]}...")

        # CSS styles
        css = '''
            <style>
            :root {
                --primary-color: #2c3e50;
                --secondary-color: #e74c3c;
                --accent-color: #f1c40f;
                --background-light: #ecf0f1;
                --text-dark: #34495e;
                --shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
            }
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            body {
                font-family: 'Hind Vadodara', sans-serif;
                background: linear-gradient(135deg, #bdc3c7, #2c3e50);
                color: var(--text-dark);
                line-height: 1.8;
                overflow-x: hidden;
            }
            .header {
                background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
                color: white;
                padding: 3rem 1rem;
                text-align: center;
                position: relative;
                box-shadow: var(--shadow);
                animation: slideInDown 1s ease-out;
            }
            .header h1 {
                font-size: 2.5rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 2px;
                text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
            }
            .news-content {
                max-width: 900px;
                margin: 3rem auto;
                padding: 2rem;
                background: white;
                border-radius: 15px;
                box-shadow: var(--shadow);
                position: relative;
                overflow: hidden;
                animation: fadeInUp 1s ease-out;
            }
            .news-content::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                width: 100%;
                height: 5px;
                background: linear-gradient(90deg, var(--secondary-color), var(--accent-color));
            }
            .news-title {
                font-size: 2.8rem;
                font-weight: 700;
                color: var(--primary-color);
                margin-bottom: 2rem;
                text-align: center;
                position: relative;
                padding-bottom: 0.5rem;
            }
            .news-title::after {
                content: '';
                position: absolute;
                bottom: 0;
                left: 50%;
                transform: translateX(-50%);
                width: 100px;
                height: 4px;
                background: var(--secondary-color);
                border-radius: 2px;
            }
            .news-paragraph {
                font-size: 1.2rem;
                font-weight: 400;
                margin: 1.5rem 0;
                text-align: justify;
                padding: 1rem;
                background: var(--background-light);
                border-radius: 10px;
                transition: transform 0.3s ease, box-shadow 0.3s ease;
            }
            .news-paragraph:hover {
                transform: translateY(-5px);
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
            }
            .news-list-item {
                font-size: 1.1rem;
                font-weight: 500;
                margin: 1rem 0;
                padding: 1.5rem;
                padding-left: 3rem;
                background: linear-gradient(135deg, #dfe6e9, #b2bec3);
                border-radius: 12px;
                position: relative;
                transition: transform 0.3s ease, background 0.3s ease;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            }
            .news-list-item::before {
                content: '➤';
                position: absolute;
                left: 1rem;
                top: 50%;
                transform: translateY(-50%);
                color: var(--accent-color);
                font-size: 1.2rem;
            }
            .news-list-item:hover {
                transform: scale(1.02);
                background: linear-gradient(135deg, #fab1a0, #e17055);
                color: white;
            }
            @keyframes slideInDown {
                from { transform: translateY(-100%); opacity: 0; }
                to { transform: translateY(0); opacity: 1; }
            }
            @keyframes fadeInUp {
                from { transform: translateY(50px); opacity: 0; }
                to { transform: translateY(0); opacity: 1; }
            }
            @media (max-width: 768px) {
                .header h1 { font-size: 2rem; }
                .news-content { margin: 1.5rem; padding: 1.5rem; }
                .news-title { font-size: 2rem; }
                .news-paragraph { font-size: 1rem; padding: 0.8rem; }
                .news-list-item { font-size: 1rem; padding: 1rem; padding-left: 2.5rem; }
            }
            @media (max-width: 480px) {
                .header h1 { font-size: 1.5rem; }
                .news-content { margin: 1rem; padding: 1rem; }
                .news-title { font-size: 1.6rem; }
                .news-paragraph { font-size: 0.9rem; }
                .news-list-item { font-size: 0.9rem; padding: 0.8rem; padding-left: 2rem; }
            }
            </style>
        '''

        # Head content with title and meta description (first paragraph)
        head_content = f'''
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{translated_content[:100] + 'અમારુ એપ ગુજરાતનુ એકમાત્ર એપ છે જે દરરોજ કેટેગરી પ્રમાણે અને પ્ર્શ્નો સહિત એટલુ કરંટ અફેર ફ્રીમા આપે છે.'}</title>            
            <link href="https://fonts.googleapis.com/css2?family=Hind+Vadodara:wght@300;400;500;600;700&display=swap" rel="stylesheet">
            {css}
        '''

        # HTML structure
        html = f'''
            <!DOCTYPE html>
            <html lang="gu">
            <head>{head_content}</head>
            <body>
                <header class="header"><h1>કરંટ અફેર ગુજરાતી</h1></header>
                <article class="news-content">
        '''

        for item in content_list:
            if not isinstance(item, dict) or 'type' not in item or 'text' not in item:
                logging.debug("Skipping invalid content item")
                continue
            text = item['text'].strip()
            if not text:
                logging.debug("Skipping empty text item")
                continue
            if item['type'] == 'heading':
                html += f'<h1 class="news-title">{text}</h1>'
            elif item['type'] == 'paragraph':
                html += f'<p class="news-paragraph">{text}</p>'
            elif item['type'] == 'list_item':
                html += f'<div class="news-list-item">{text}</div>'
        html += '</article></body></html>'
        logging.debug("HTML formatting completed")
        return html
    except Exception as e:
        logging.error(f"HTML formatting error: {e}")
        logging.error(traceback.format_exc())
        return ""

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), retry=retry_if_exception_type(mysql.connector.Error))
def insert_news(connection, cat_id, news_title, news_description, news_image):
    if not news_title or len(news_title.strip()) == 0:
        logging.error("Cannot insert news with empty title")
        return False
    query = """
    INSERT INTO tbl_news (cat_id, news_title, news_date, news_description, news_image, news_status, video_url, video_id, content_type, size, view_count, last_update)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = (cat_id, news_title, current_timestamp, news_description, news_image, 1, "", "", "Post", "", 0, current_timestamp)
    try:
        connection = check_and_reconnect(connection)
        if not connection:
            logging.error("Database connection is None")
            return False
        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        cursor.close()
        logging.info(f"News inserted successfully: {news_title}")
        return True
    except mysql.connector.Error as err:
        logging.error(f"Error inserting news: {err}")
        logging.error(traceback.format_exc())
        raise

def send_promotional_message(channel_username, bot_token, article_titles):
    try:
        current_date = datetime.now().strftime('%d %B %Y')
        message = f"🌟 **{current_date} - Current Affairs in Gujarati** 🌟\n\nHere are the latest updates:\n"
        for i, title in enumerate(article_titles, 1):
            message += f"\n{['📌', '🌟', '💡'][i % 3]} **{title}**"
        message += "\n\n📱 **Download our App**: [Link]\n📣 **Join Telegram**: [https://t.me/gujtest]"
        logging.debug(f"Preparing Telegram message: {message[:100]}...")
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        params = {'chat_id': channel_username, 'text': message, 'parse_mode': 'Markdown'}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            logging.info("Telegram message sent successfully")
        else:
            logging.error(f"Failed to send Telegram message: {response.text}")
    except Exception as e:
        logging.error(f"Error sending Telegram message: {e}")
        logging.error(traceback.format_exc())

class FirebaseNotificationSender:
    def __init__(self, topic=None):
        self.fcm_notification_topic = topic or os.getenv('FCM_NOTIFICATION_TOPIC', 'android_news_app_topic')
        logging.debug(f"Firebase sender initialized with topic: {self.fcm_notification_topic}")

    def send_notification(self, title, message, image_url=None):
        try:
            notification = messaging.Notification(title=title, body=message, image=image_url)
            data = {"id": str(random.randint(1000, 9999)), "title": title, "message": message}
            if image_url:
                data["image"] = image_url
            fcm_message = messaging.Message(notification=notification, data=data, topic=self.fcm_notification_topic)
            response = messaging.send(fcm_message)
            logging.info(f"Firebase notification sent: {response}")
            return True, response
        except Exception as e:
            logging.error(f"Failed to send Firebase notification: {e}")
            logging.error(traceback.format_exc())
            return False, str(e)

def send_post_notification(combined_title, paragraph, image_name):
    try:
        image_url = f"https://newsadmin.currentadda.com/upload/{image_name}" if image_name else None
        sender = FirebaseNotificationSender()
        clean_title = combined_title[:100] if combined_title else "Current Affairs Update"
        clean_paragraph = paragraph[:200] if paragraph else "New update available."
        logging.debug(f"Sending Firebase notification with title: {clean_title[:50]}...")
        success, response = sender.send_notification(clean_title, clean_paragraph, image_url)
        if success:
            logging.info(f"Firebase notification sent successfully: {response}")
        else:
            logging.error(f"Firebase notification failed: {response}")
        return success
    except Exception as e:
        logging.error(f"Error in send_post_notification: {e}")
        logging.error(traceback.format_exc())
        return False

async def scrape_and_process_article(url, connection, mongo_collection, article_titles):
    try:
        if is_url_scraped(mongo_collection, url):
            logging.info(f"URL already scraped, skipping: {url}")
            return False, None
        
        logging.info(f"Processing article: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        main_content = (soup.find('div', class_='inside_post column content_width') or
                        soup.find('article') or
                        soup.find('div', class_='content'))
        if not main_content:
            logging.error("No main content found")
            return False, None
        first_paragraph = main_content.find('p')
        first_paragraph_translated = translate_to_gujarati(first_paragraph.get_text().strip()) if first_paragraph else "No introductory text available."
        logging.debug(f"First paragraph translated: {first_paragraph_translated[:50]}...")
        featured_image_div = soup.find('div', class_='featured_image')
        image_filename = None
        if featured_image_div and (img_tag := featured_image_div.find('img')) and img_tag.get('src'):
            image_filename = download_and_process_image(img_tag['src'])
        heading = (main_content.find('h1', id='list') or main_content.find('h1') or soup.find('title'))
        if not heading:
            logging.error("No heading found")
            return False, None
        original_heading = heading.get_text().strip()
        translated_heading = translate_to_gujarati(original_heading)
        combined_heading = f"{original_heading} - {translated_heading}"
        logging.debug(f"Combined heading: {combined_heading[:50]}...")
        content_list = [{'type': 'heading', 'text': combined_heading}]
        for tag in main_content.find_all(recursive=False):
            if tag.get('class') in [['sharethis-inline-share-buttons'], ['prenext']]:
                continue
            text = tag.get_text().strip()
            if not text:
                continue
            translated_text = translate_to_gujarati(text)
            if tag.name == 'p':
                content_list.append({'type': 'paragraph', 'text': translated_text})
            elif tag.name == 'ul':
                for li in tag.find_all('li'):
                    if li_text := li.get_text().strip():
                        content_list.append({'type': 'list_item', 'text': translate_to_gujarati(li_text)})
        news_description = " ".join(item['text'] for item in content_list if item['type'] == 'paragraph')
        cat_id = next((id for cat, id in CATEGORY_MAP.items() if cat in news_description), 1)
        formatted_html = format_content_as_html(content_list)
        if not formatted_html:
            logging.error("Failed to format HTML content")
            return False, None
        success = insert_news(connection, cat_id, combined_heading, formatted_html, image_filename)
        if success and combined_heading:
            send_post_notification(combined_heading, first_paragraph_translated, image_filename)
            log_scraped_url(mongo_collection, url)  # Log URL to MongoDB after successful processing
            return True, combined_heading
        logging.warning("Failed to insert news into database")
        return False, None
    except Exception as e:
        logging.error(f"Error processing article {url}: {e}")
        logging.error(traceback.format_exc())
        return False, None

async def main():
    connection = None
    mongo_client = None
    try:
        logging.info("Starting news scraper")
        connection = create_db_connection()
        if not connection:
            raise Exception("Failed to establish initial database connection")
        
        mongo_client, mongo_collection = create_mongo_connection()
        if not mongo_client:
            raise Exception("Failed to establish MongoDB connection")
        
        base_url = "https://www.gktoday.in/current-affairs/"
        article_urls = fetch_article_urls(base_url, 1)
        article_titles = []
        for url in article_urls:
            if 'daily-current-affairs-quiz' not in url:
                success, combined_title = await scrape_and_process_article(url, connection, mongo_collection, article_titles)
                if success and combined_title:
                    article_titles.append(combined_title)
                    logging.debug(f"Added title to list: {combined_title[:50]}...")
                await asyncio.sleep(2)
        if article_titles:
            logging.info(f"Sending Telegram message with {len(article_titles)} titles")
            send_promotional_message(TELEGRAM_CHANNEL, TELEGRAM_BOT_TOKEN, article_titles)
        else:
            logging.warning("No article titles to send in Telegram message")
    except Exception as e:
        logging.error(f"Main execution error: {e}")
        logging.error(traceback.format_exc())
        raise
    finally:
        if connection:
            connection.close()
            logging.info("Database connection closed")
        if mongo_client:
            mongo_client.close()
            logging.info("MongoDB connection closed")

def run_scraper():
    try:
        logging.info("Running scraper")
        asyncio.run(main())
        logging.info("Scraper completed successfully")
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error in run_scraper: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    run_scraper()
