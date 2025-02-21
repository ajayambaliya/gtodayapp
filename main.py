import io
import os
import requests
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime
from deep_translator import GoogleTranslator, exceptions
import asyncio
import logging
import tempfile
from PIL import Image
import ftplib
from urllib.parse import urlparse
import hashlib
import traceback
import datetime
import sys
import random
import json
import firebase_admin
from firebase_admin import credentials, messaging
from dotenv import load_dotenv


# Configure detailed logging with UTF-8 encoding
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('news_scraper.log', encoding='utf-8')  # Set encoding to UTF-8
    ]
)


# Database configuration (modified to use environment variables)
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

port_str = os.getenv('FTP_PORT', '').strip()
if not port_str.isdigit():
    port_str = '21'
port = int(port_str)

FTP_CONFIG = {
    'host': os.getenv('FTP_HOST'),
    'user': os.getenv('FTP_USER'),
    'password': os.getenv('FTP_PASSWORD'),
    'port': port,
    'upload_path': os.getenv('FTP_UPLOAD_PATH', '/')
}


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
}
CUSTOM_TRANSLATION_MAP = {

    "Science and Technology Current Affairs": "વિજ્ઞાન અને ટેકનોલોજી વર્તમાન બાબતો",
    "Defense Current Affairs": "સંરક્ષણ વર્તમાન બાબતો",
    "Legal and Constitutional Current Affairs": "કાનૂની અને બંધારણ વર્તમાન બાબતો",
    "Environmental Current Affairs": "પર્યાવરણ વર્તમાન બાબતો",
    "Government Schemes Current Affairs": "સરકારી યોજનાઓ વર્તમાન બાબતો",
    "Economy and Banking Current Affairs": "અર્થતંત્ર અને બેંકિંગ વર્તમાન બાબતો",
    "International/World Current Affairs": "આંતરરાષ્ટ્રીય/વિશ્વ વર્તમાન બાબતો",
    "Summits and Conferences": "સમિટ અને પરિષદો",
    "Important Days and Events Current Affairs": "મહત્વપૂર્ણ દિવસો અને ઘટનાઓ વર્તમાન બાબતો",
    "Reports and Indices Current Affairs": "અહેવાલો અને સૂચકાંકો વર્તમાન બાબતો",
    "Sports Current Affairs": "રમતગમત વર્તમાન બાબતો",
    "Awards, Honors and Persons in News": "સમાચારમાં પુરસ્કારો, સન્માનો અને વ્યક્તિઓ",
    "Agriculture Current Affairs": "કૃષિ વર્તમાન બાબતો",
    "Art and Culture Current Affairs": "કલા અને સંસ્કૃતિ વર્તમાન બાબતો",
    # Add more terms/phrases here...
}


# Load environment variables
load_dotenv()

def create_db_connection():
    try:
        logging.info("Attempting to create database connection...")
        connection = mysql.connector.connect(**DB_CONFIG)
        connection.ping(reconnect=True)  # Enable automatic reconnection
        logging.info("Database connection successful")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
        logging.error(f"Connection attempt details: host={DB_CONFIG['host']}, user={DB_CONFIG['user']}, database={DB_CONFIG['database']}")
        return None

def check_and_reconnect(connection):
    try:
        logging.debug("Checking database connection...")
        if connection and connection.is_connected():
            connection.ping(reconnect=True)
            logging.debug("Existing connection is valid")
            return connection
        logging.info("Connection lost, attempting to reconnect...")
        return create_db_connection()
    except mysql.connector.Error as err:
        logging.error(f"Error during connection check: {err}")
        return create_db_connection()

def fetch_article_urls(base_url, pages):
    article_urls = []
    logging.info(f"Starting to fetch articles from {base_url} for {pages} pages")
    session = requests.Session()  # Use session for better performance
    
    for page in range(1, pages + 1):
        url = base_url if page == 1 else f"{base_url}page/{page}/"
        try:
            logging.debug(f"Fetching page {page}: {url}")
            response = session.get(url, timeout=30)  # Added timeout
            response.raise_for_status()  # Raise exception for bad status codes
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_articles = [a_tag['href'] for h1_tag in soup.find_all('h1', id='list') 
                           if (a_tag := h1_tag.find('a')) and a_tag.get('href')]
            
            logging.info(f"Found {len(page_articles)} articles on page {page}")
            article_urls.extend(page_articles)
            
        except requests.RequestException as e:
            logging.error(f"Error fetching page {page}: {e}")
            logging.error(traceback.format_exc())
            continue  # Continue with next page on error
            
    return article_urls

translation_cache = {}

def translate_to_gujarati(text):
    # Quick return if text is empty or whitespace
    if not text or not text.strip():
        return text

    # Apply custom translations BEFORE calling Google Translate
    # This ensures that these phrases will not be further changed by Google Translate
    for en_term, gu_term in CUSTOM_TRANSLATION_MAP.items():
        text = text.replace(en_term, gu_term)

    # Check cache
    if text in translation_cache:
        return translation_cache[text]

    try:
        logging.debug(f"Translating text (first 50 chars): {text[:50]}...")
        translator = GoogleTranslator(source='auto', target='gu')
        translated = translator.translate(text)
        translation_cache[text] = translated  # Cache the translated result
        logging.debug("Translation successful")
        return translated
    except Exception as e:
        # If an error occurs with Google Translate, log a warning and return the original text
        logging.warning(f"Translation error: {e}, returning original text")
        logging.warning(traceback.format_exc())
        return text


def download_and_process_image(image_url):
    temp_file_path = None
    ftp = None
    
    try:
        logging.info(f"Starting image download from: {image_url}")
        
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp_file:
            temp_file_path = tmp_file.name
            logging.debug(f"Created temporary file: {temp_file_path}")
            
            # Process image
            img = Image.open(io.BytesIO(response.content))
            img = img.convert('RGB')
            img.save(temp_file_path, 'PNG')
            
            # Generate filename
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            hash_object = hashlib.md5(image_url.encode())
            filename = f"news_{timestamp}_{hash_object.hexdigest()[:8]}.png"
            
            # FTP Upload
            logging.info("Initiating FTP connection...")
            ftp = ftplib.FTP()
            ftp.connect(FTP_CONFIG['host'], FTP_CONFIG['port'])
            logging.info("FTP connection established")
            
            logging.debug("Attempting FTP login...")
            ftp.login(FTP_CONFIG['user'], FTP_CONFIG['password'])
            logging.info("FTP login successful")
            
            logging.debug(f"Changing to upload directory: {FTP_CONFIG['upload_path']}")
            ftp.cwd(FTP_CONFIG['upload_path'])
            
            logging.debug("Starting file upload...")
            with open(temp_file_path, 'rb') as file:
                ftp.storbinary(f'STOR {filename}', file)
            logging.info("File upload completed")
            
            return filename
            
    except requests.RequestException as e:
        logging.error(f"Error downloading image: {e}")
        return None
    except (IOError, OSError) as e:
        logging.error(f"Error processing image: {e}")
        return None
    except ftplib.all_errors as e:
        logging.error(f"FTP error: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        logging.error(traceback.format_exc())
        return None
    finally:
        if ftp and ftp.sock:
            try:
                ftp.quit()
                logging.debug("FTP connection closed")
            except:
                pass
                
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logging.debug("Temporary file deleted")
            except Exception as e:
                logging.error(f"Error deleting temporary file: {e}")

def format_content_as_html(content_list):
    try:
        logging.debug("Starting HTML formatting")
        
        # Extract first 100 characters of translated content
        translated_content = ""
        translated_first_paragraph = ""
        
        for item in content_list:
            if item.get('type') in ['heading', 'paragraph'] and item.get('text'):
                translated_content = item['text']
                if item['type'] == 'paragraph' and not translated_first_paragraph:
                    translated_first_paragraph = item['text']
                break
        
        title_text = translated_content[:100] if translated_content else "Article"
        
        head_content = f'''
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title_text} - {translated_first_paragraph}</title>
            <link rel="preconnect" href="https://fonts.googleapis.com">
            <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
            <link href="https://fonts.googleapis.com/css2?family=Noto+Serif+Gujarati:wght@400;500;600;700&display=swap" rel="stylesheet">
        '''
        
        # Current Comment: Change whole styling as per new requirements
        # Removed the previous excessive styling and added new CSS as per user request.

        css = '''
            <style>
            /* Best App FOr Current Affairs in Gujarati to pass any government exams. */
            :root {
                --primary-color: #1a237e;
                --secondary-color: #303f9f;
                --accent-color: #3949ab;
                --accent-light: #e8eaf6;
                --text-color: #333;
                --text-light: #666;
                --background-light: #f8f9fa;
                --success-color: #4caf50;
                --warning-color: #ff9800;
                --white: #ffffff;
                --spacing-unit: 1rem;
                --border-radius: 8px;
                --box-shadow: 0 4px 6px rgba(0,0,0,0.05);
            }

            /* Global Styles */
            body {
                margin: 0;
                padding: 0;
                background-color: var(--background-light);
                font-family: 'Noto Serif Gujarati', serif;
                line-height: 1.6;
                color: var(--text-color);
            }

            /* Enhanced Header */
            .header {
                background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
                color: var(--white);
                padding: calc(var(--spacing-unit) * 2) 0;
                text-align: center;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                position: relative;
                overflow: hidden;
            }

            .header::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: radial-gradient(circle at 30% 50%, rgba(255,255,255,0.1) 0%, transparent 50%);
            }

            .header h1 {
                margin: 0;
                font-size: 2.5em;
                font-weight: 700;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
                position: relative;
            }

            /* Main Content Container */
            .news-content {
                max-width: 800px;
                margin: 2rem auto;
                padding: 2rem;
                background: var(--white);
                border-radius: var(--border-radius);
                box-shadow: var(--box-shadow);
                position: relative;
            }

            .news-content::before {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 4px;
                background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
                border-radius: var(--border-radius) var(--border-radius) 0 0;
            }

            /* Enhanced Typography */
            .news-title {
                font-size: 2.5em;
                color: var(--primary-color);
                margin: 1.5rem 0;
                font-weight: 700;
                line-height: 1.3;
                text-align: center;
                padding: 1rem 0;
                position: relative;
            }

            .news-title::after {
                content: '';
                display: block;
                width: 100px;
                height: 4px;
                background: linear-gradient(90deg, var(--accent-color), transparent);
                margin: 1rem auto 0;
                border-radius: 2px;
            }

            .news-subtitle {
                font-size: 1.8em;
                color: var(--secondary-color);
                margin: 2rem 0 1rem;
                font-weight: 600;
                padding: 0.5rem 1rem;
                background: var(--accent-light);
                border-radius: var(--border-radius);
                box-shadow: inset 0 0 0 1px rgba(0,0,0,0.05);
            }

            .news-section {
                font-size: 1.4em;
                color: var(--accent-color);
                margin: 1.5rem 0 1rem;
                font-weight: 500;
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }

            .news-section::before {
                content: '📌';
                font-size: 0.9em;
            }

            /* Enhanced Paragraphs */
            .news-paragraph {
                margin: 1.2rem 0;
                font-size: 1.1em;
                text-align: justify;
                overflow-wrap: break-word;
                line-height: 1.8;
                padding: 0.5rem;
                border-radius: var(--border-radius);
                transition: background-color 0.3s ease;
            }

            .news-paragraph:hover {
                background-color: var(--accent-light);
            }

            /* Enhanced List Items */
            .news-list-item {
                margin: 1rem 0;
                padding: 1rem 1.5rem;
                position: relative;
                font-size: 1.1em;
                background: var(--background-light);
                border-radius: var(--border-radius);
                border-left: 4px solid var(--accent-color);
                transition: transform 0.2s ease, box-shadow 0.2s ease;
            }

            .news-list-item:hover {
                transform: translateX(5px);
                box-shadow: var(--box-shadow);
            }

            .news-list-item::before {
                content: "→";
                color: var(--accent-color);
                font-weight: bold;
                position: absolute;
                left: 0.5rem;
                top: 50%;
                transform: translateY(-50%);
            }

            /* Enhanced Image Container */
            .image-container {
                max-width: 100%;
                margin: 2rem 0;
                text-align: center;
                padding: 1rem;
                background: var(--background-light);
                border-radius: var(--border-radius);
            }

            .image-container img {
                max-width: 100%;
                height: auto;
                border-radius: calc(var(--border-radius) - 2px);
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                transition: transform 0.3s ease, box-shadow 0.3s ease;
            }

            .image-container img:hover {
                transform: scale(1.02) translateY(-5px);
                box-shadow: 0 8px 20px rgba(0,0,0,0.15);
            }

            /* Enhanced Meta Information */
            .meta-info {
                display: flex;
                justify-content: space-around;
                align-items: center;
                margin-top: 3rem;
                padding: 1.5rem;
                background: var(--accent-light);
                border-radius: var(--border-radius);
                box-shadow: inset 0 0 0 1px rgba(0,0,0,0.05);
            }

            .meta-info span {
                display: flex;
                align-items: center;
                gap: 0.5rem;
                color: var(--text-color);
                font-size: 0.9em;
                padding: 0.5rem 1rem;
                background: var(--white);
                border-radius: calc(var(--border-radius) - 2px);
                box-shadow: var(--box-shadow);
                transition: transform 0.2s ease;
            }

            .meta-info span:hover {
                transform: translateY(-2px);
            }

            

            /* Highlight Box */
            .highlight-box {
                background: linear-gradient(45deg, var(--accent-light), var(--white));
                border-left: 4px solid var(--accent-color);
                padding: 1.5rem;
                margin: 2rem 0;
                border-radius: 0 var(--border-radius) var(--border-radius) 0;
                box-shadow: var(--box-shadow);
                position: relative;
                overflow: hidden;
            }

            .highlight-box::before {
                content: '💡';
                position: absolute;
                top: 1rem;
                right: 1rem;
                font-size: 1.5em;
                opacity: 0.2;
            }

            /* Responsive Design */
            @media (max-width: 768px) {
                .news-content {
                    margin: 1rem;
                    padding: 1.5rem;
                }

                .news-title {
                    font-size: 2em;
                }

                .meta-info {
                    flex-direction: column;
                    gap: 1rem;
                }

                .meta-info span {
                    width: 100%;
                    justify-content: center;
                }
            }

            @media (max-width: 480px) {
                .header h1 {
                    font-size: 2em;
                }

                .news-content {
                    margin: 0.5rem;
                    padding: 1rem;
                }

                .news-title {
                    font-size: 1.8em;
                }

                .news-subtitle {
                    font-size: 1.3em;
                }

                .news-paragraph,
                .news-list-item {
                    font-size: 1em;
                }
            }

            /* Print Styles */
            @media print {
                .header {
                    background: none;
                    color: var(--text-color);
                    padding: 1rem 0;
                }

                .news-content {
                    box-shadow: none;
                    margin: 0;
                    padding: 1rem;
                }

                .share-buttons,
                .meta-info {
                    display: none;
                }
            }

            /* Animations */
            @keyframes fadeIn {
                from { 
                    opacity: 0;
                    transform: translateY(20px);
                }
                to {
                    opacity: 1;
                    transform: translateY(0);
                }
            }

            @keyframes slideIn {
                from {
                    opacity: 0;
                    transform: translateX(-20px);
                }
                to {
                    opacity: 1;
                    transform: translateX(0);
                }
            }

            .news-content {
                animation: fadeIn 0.5s ease-out;
            }

            .news-list-item {
                animation: slideIn 0.3s ease-out;
            }
            </style>
        '''
        
        html = f'''
            <!DOCTYPE html>
            <html lang="gu">
            <head>
                {head_content}
                {css}
            </head>
            <body>
                <header class="header">
                    <h1>કરંટ અફેર ગુજરાતી </h1>
                </header>
                <article class="news-content">
        '''
        
        is_first_paragraph = True
        for item in content_list:
            if not isinstance(item, dict) or 'type' not in item or 'text' not in item:
                continue
                
            text = item['text'].strip()
            if not text:
                continue
                
            if item['type'] == 'heading':
                html += f'<h1 class="news-title">{text}</h1>'
            elif item['type'] == 'paragraph':
                if is_first_paragraph:
                    html += f'<p class="news-paragraph">{text}</p>'
                    is_first_paragraph = False
                else:
                    html += f'<p class="news-paragraph">{text}</p>'
            elif item['type'] == 'heading_2':
                html += f'<h2 class="news-subtitle">{text}</h2>'
            elif item['type'] == 'list_item':
                html += f'<div class="news-list-item">{text}</div>'
        
        html += '''
                    <div class="meta-info">
                        <span>👁️ વાંચન સમય: 5 મિનિટ</span>
                    </div>
                </article>
            </body>
            </html>
        '''
        
        return html
        
    except Exception as e:
        logging.error(f"Error in HTML formatting: {e}")
        logging.error(traceback.format_exc())
        return ""





def insert_news(connection, cat_id, news_title, news_description, news_image):
    if not news_title or len(news_title.strip()) == 0:
        logging.error("Cannot insert news with empty title")
        return False
        
    logging.info(f"Inserting news with title: {news_title[:50]}...")

    query = """
    INSERT INTO tbl_news (cat_id, news_title, news_date, news_description, news_image, 
                         news_status, video_url, video_id, content_type, size, view_count, last_update)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


    data = (
        cat_id,
        news_title,
        current_timestamp,
        news_description,
        news_image,
        1,
        "",
        "",
        "Post",
        "",
        0,
        current_timestamp
    )

    try:
        connection = check_and_reconnect(connection)
        if not connection:
            logging.error("Failed to establish database connection")
            return False

        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        cursor.close()
        logging.info("News inserted successfully")
        return True

    except mysql.connector.Error as err:
        logging.error(f"Error inserting news: {err}")
        logging.error(traceback.format_exc())
        return False




# Function to send promotional message to Telegram
def send_promotional_message(channel_username, bot_token, article_titles):
    try:
        # Format the current date in "dd Month yyyy" format
        current_date = datetime.datetime.now().strftime('%d %B %Y')
        
        # Start the message content with the current date
        message = f"""
🌟 **{current_date} - Current Affairs in Gujarati** 🌟

Here are the latest updates for you:
"""
        
        # Add each article title with a symbol
        for i, title in enumerate(article_titles, start=1):
            message += f"\n{emoji_choice(i)} **{title}**"  # Adding bold formatting for titles

        # Add a call-to-action
        message += """
        
📱 **Download our Android App for Reading Current Affairs**:
[App Link] - (provide the app link here)

📣 **Join our Telegram Channel**:
[Telegram Channel](https://t.me/gujtest) for instant updates and discussions!

📰 Stay updated, stay ahead!
"""
        
        # Send the message using Telegram Bot API
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        params = {
            'chat_id': channel_username,
            'text': message,
            'parse_mode': 'Markdown',  # Use Markdown formatting for bold, italics, etc.
            'disable_web_page_preview': True
        }
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            logging.info("Promotional message sent successfully to Telegram.")
        else:
            logging.error(f"Failed to send promotional message: {response.text}")
    
    except Exception as e:
        logging.error(f"Error sending promotional message: {e}")

# Function to get a symbol for each article
def emoji_choice(index):
    emojis = ['📌', '🌟', '💡', '🔥', '🎯', '💥', '⭐', '🚀', '⚡', '💫']
    return emojis[(index - 1) % len(emojis)]

def send_post_notification(title, paragraph, image_name):
    """Send notification for a newly scraped post with image"""
    try:
        # Format image URL properly if image exists
        image_url = None
        if image_name and isinstance(image_name, str) and len(image_name.strip()) > 0:
            image_url = f"https://news.dalalstreetgujarati.in/upload/{image_name}"
            logging.info(f"Including image in notification: {image_url}")
        else:
            logging.info("No image available for notification")
        
        # Get Firebase notification sender instance
        sender = FirebaseNotificationSender()
        
        # Clean up notification content
        clean_title = title[:100] if title else "Current Affairs Update"
        clean_paragraph = paragraph[:200] if paragraph else "New current affairs update available."
        
        # Send the notification with image
        success, response = sender.send_notification(
            title=clean_title, 
            message=clean_paragraph, 
            image_url=image_url
        )
        
        if success:
            logging.info(f"Firebase notification sent successfully with{'out' if not image_url else ''} image: {response}")
        else:
            logging.error(f"Failed to send Firebase notification: {response}")
        
        return success
    except Exception as e:
        logging.error(f"Error in send_post_notification: {e}")
        logging.error(traceback.format_exc())
        return False


class FirebaseNotificationSender:
    def __init__(self, service_account_json=None, topic=None):
        """Initialize the Firebase notification sender with credential options."""
        try:
            # Try to get topic from env var if not provided
            self.fcm_notification_topic = topic or os.getenv('FCM_NOTIFICATION_TOPIC', 'android_news_app_topic')
            
            # Handle service account initialization
            self.app = self._initialize_firebase(service_account_json)
            
            logging.info(f"Using notification topic: {self.fcm_notification_topic}")
            logging.info("Firebase initialized successfully")
                
        except Exception as e:
            logging.error(f"Firebase initialization error: {e}")
            logging.error(traceback.format_exc())
    
    def _initialize_firebase(self, service_account_json=None):
        """Initialize Firebase with credentials from various sources."""
        try:
            # Option 1: Direct JSON string provided
            if service_account_json:
                cred_dict = json.loads(service_account_json)
                cred = credentials.Certificate(cred_dict)
                return firebase_admin.initialize_app(cred)
            
            # Option 2: JSON from environment variable
            service_account_env = os.getenv('FIREBASE_SERVICE_ACCOUNT')
            if service_account_env:
                try:
                    cred_dict = json.loads(service_account_env)
                    cred = credentials.Certificate(cred_dict)
                    return firebase_admin.initialize_app(cred)
                except json.JSONDecodeError:
                    logging.error("Error: FIREBASE_SERVICE_ACCOUNT environment variable contains invalid JSON")
            
            # Get script directory for relative paths
            script_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Option 3: Path from environment variable
            service_account_path_env = os.getenv('FIREBASE_SERVICE_ACCOUNT_PATH')
            if service_account_path_env:
                path = service_account_path_env
                if not os.path.isabs(path):
                    path = os.path.join(script_dir, path)
                if os.path.exists(path):
                    cred = credentials.Certificate(path)
                    return firebase_admin.initialize_app(cred)
                logging.warning(f"Service account file not found at {path}")
            
            # Option 4: Default path
            default_path = os.path.join(script_dir, 'service-account.json')
            if os.path.exists(default_path):
                logging.info(f"Using service account at: {default_path}")
                cred = credentials.Certificate(default_path)
                return firebase_admin.initialize_app(cred)
            
            raise ValueError("No valid Firebase credentials found. Please configure service account credentials.")
            
        except Exception as e:
            logging.error(f"Firebase initialization error: {e}")
            logging.error(traceback.format_exc())
            return None
    
    def send_notification(self, title, message, image_url=None, link=None, post_id=0):
        """Send notification via Firebase Cloud Messaging with image support."""
        try:
            # Generate unique ID
            unique_id = str(random.randint(1000, 9999))
            
            # Log image URL if present
            if image_url:
                logging.info(f"Preparing notification with image: {image_url}")
            
            # Prepare notification message with image
            notification = messaging.Notification(
                title=title,
                body=message,
                image=image_url  # FCM supports image URLs directly in notification
            )
            
            # Prepare data payload (include image URL in data payload too for devices that don't support notification images)
            data = {
                "id": unique_id,
                "title": title,
                "message": message,
                "post_id": str(post_id),
                "link": link or ""
            }
            
            if image_url:
                data["image"] = image_url
            
            # Create message to topic
            fcm_message = messaging.Message(
                notification=notification,
                data=data,
                topic=self.fcm_notification_topic
            )
            
            # Send message
            response = messaging.send(fcm_message)
            logging.info(f"Notification sent successfully. Message ID: {response}")
            return True, response
            
        except Exception as e:
            error_msg = f"Failed to send notification: {e}"
            logging.error(error_msg)
            logging.error(traceback.format_exc())
            return False, error_msg


async def scrape_and_process_article(url, connection, article_titles):
    translated_heading = None
    first_paragraph_translated = None
    image_filename = None
    
    try:
        logging.info(f"Processing article: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract main content
        main_content = soup.find('div', class_='inside_post column content_width')
        if not main_content:
            logging.error("Main content div not found")
            return False, None
        
        # Extract the first paragraph and translate it
        first_paragraph = main_content.find('p')
        if first_paragraph:
            first_paragraph_text = first_paragraph.get_text().strip()
            first_paragraph_translated = translate_to_gujarati(first_paragraph_text)
            logging.info(f"First Paragraph (Translated): {first_paragraph_translated[:50]}...")
        
        # Extract featured image
        featured_image_div = soup.find('div', class_='featured_image')
        image_url = None
        if featured_image_div:
            img_tag = featured_image_div.find('img')
            if img_tag and img_tag.get('src'):
                image_url = img_tag['src']
                logging.info(f"Found image URL: {image_url}")
        
        # Process image
        if image_url:
            logging.info("Starting image processing")
            image_filename = download_and_process_image(image_url)
            if image_filename:
                logging.info(f"Image processed successfully: {image_filename}")
            else:
                logging.warning("Image processing failed")
        
        # Extract and process content
        content_list = []
        heading = main_content.find('h1', id='list')
        if not heading:
            logging.error("Article heading not found")
            return False, None
        
        # Process heading
        heading_text = heading.get_text().strip()
        if heading_text:
            logging.info(f"Processing article heading: {heading_text[:50]}...")
            translated_heading = translate_to_gujarati(heading_text)
            content_list.append({'type': 'heading', 'text': translated_heading})
        else:
            logging.error("No heading text found")
            return False, None
        
        # Process main content
        for tag in main_content.find_all(recursive=False):
            if tag.get('class') in [['sharethis-inline-share-buttons'], ['prenext']]:
                continue
                
            text = tag.get_text().strip()
            if not text:
                continue
                
            translated_text = translate_to_gujarati(text)
            
            if tag.name == 'p':
                content_list.append({'type': 'paragraph', 'text': translated_text})
            elif tag.name == 'h2':
                content_list.append({'type': 'heading_2', 'text': translated_text})
            elif tag.name == 'h4':
                content_list.append({'type': 'heading_4', 'text': translated_text})
            elif tag.name == 'ul':
                for li in tag.find_all('li'):
                    li_text = li.get_text().strip()
                    if li_text:
                        translated_li_text = translate_to_gujarati(li_text)
                        content_list.append({'type': 'list_item', 'text': translated_li_text})

        # Extract news description
        news_description = " ".join([item['text'] for item in content_list if item['type'] == 'paragraph'])

        # Check for category in news description
        cat_id = None
        for category, id in CATEGORY_MAP.items():
            if category in news_description:
                cat_id = id
                logging.info(f"Category found: {category}, ID: {id}")
                break
        
        if not cat_id:
            logging.warning("No matching category found in news description")

        # Format and insert content
        formatted_html = format_content_as_html(content_list)
        if not formatted_html:
            logging.error("Failed to format content as HTML")
            return False, None

        # Insert into the database
        success = insert_news(
            connection,
            cat_id if cat_id else 1,
            translated_heading if translated_heading else '',
            formatted_html,
            image_filename if image_filename else ''
        )
        
        if success:
            logging.info("Article processed and inserted successfully")
            
            # Send Firebase notification for the new article with image
            if translated_heading and first_paragraph_translated:
                notification_success = send_post_notification(
                    translated_heading,
                    first_paragraph_translated,
                    image_filename  # Pass image filename for notification
                )
                if notification_success:
                    logging.info("Firebase notification sent successfully")
                else:
                    logging.warning("Firebase notification sending failed")
        else:
            logging.error("Failed to insert article into database")
        
        return success, translated_heading

    except requests.RequestException as e:
        logging.error(f"Error fetching article {url}: {e}")
        return False, None
    except Exception as e:
        logging.error(f"Error processing article {url}: {e}")
        logging.error(traceback.format_exc())
        return False, None


async def main():
    connection = None
    article_titles = []  # Initialize list here

    try:
        logging.info("Starting news scraper")
        
        # Create database connection
        connection = create_db_connection()
        if not connection:
            raise Exception("Failed to establish initial database connection")
        
        # Fetch article URLs
        base_url = "https://www.gktoday.in/current-affairs/"
        logging.info(f"Fetching articles from {base_url}")
        article_urls = fetch_article_urls(base_url, 1)
        logging.info(f"Found {len(article_urls)} articles to process")
        
        article_titles = []  # To store article titles
        
        # Process articles
        for url in article_urls:
            if 'daily-current-affairs-quiz' not in url:
                success, translated_heading = await scrape_and_process_article(url, connection, article_titles)  # Pass the list
                if success and translated_heading:
                    article_titles.append(translated_heading)
                    
                # Add a small delay between articles to avoid overwhelming the servers
                await asyncio.sleep(2)
        
        # Send the promotional message once all scraping is done
        if article_titles:
            send_promotional_message(os.getenv('TELEGRAM_CHANNEL'),os.getenv('TELEGRAM_BOT_TOKEN'),article_titles)
        
        logging.info("Finished processing all articles")
        
    except Exception as e:
        logging.error(f"Main execution error: {e}")
        logging.error(traceback.format_exc())
        raise
    finally:
        # Cleanup
        if connection:
            try:
                connection.close()
                logging.info("Database connection closed")
            except mysql.connector.Error as err:
                logging.error(f"Error closing database connection: {err}")


def run_scraper():
    """Wrapper function to handle running the async main function"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error in run_scraper: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)
        sys.exit(1)

if __name__ == "__main__":
    import sys
    run_scraper()
