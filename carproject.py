import time
import logging
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import mysql.connector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

cnx = mysql.connector.connect(user = 'root',
                              password = 'password',
                              database = 'carproject',
                              host='127.0.0.1')
headers = {
    "User-Agent": "Mozilla/5.0"}
cursor = cnx.cursor()
"""
cursor.execute('SELECT * FROM cars')
rows = cursor.fetchall()"""

def reindexing_ids():
    cursor.execute('SELECT * FROM cars')
    rows = cursor.fetchall()
    counter = 1
    for link in rows:
        cursor.execute('UPDATE cars SET i = %s WHERE link = %s;',(counter,link[1]))
        counter += 1

def clean_expired_links():
    cursor.execute('SELECT * FROM cars')
    rows = cursor.fetchall()
    for link in rows:
        try:
            result = requests.get(link[1], headers=headers, timeout=10)
            if result.status_code != 200:
                cursor.execute('DELETE FROM cars WHERE link = %s;', (link[1],))
                logging.info(f"Deleted expired link from DB: {link[1]}")
        except requests.RequestException as e:
            logging.warning(f"Request failed for {link[1]}: {e}")

def get_next_id():
    i = 1
    cursor.execute("SELECT i FROM cars ORDER BY i DESC LIMIT 1")
    last_data = cursor.fetchone()
    last_data = (0,) if last_data == None else last_data
    i = last_data[0]+1 
    return i

def main():
    try:
        driver = webdriver.Chrome()
        driver.get('https://bama.ir/car/peugeot-405')
    except:
        logging.error('something went wrong')
        quit()
    # scraping each link        
    for _ in range(10):
        driver.find_element(By.TAG_NAME,'body').send_keys(Keys.END)
        time.sleep(2)
    html = driver.page_source
    driver.close()
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all('a',class_='bama-ad listing')

    # limiting the entries
    links = links[:20] if len(links) > 100 else links

    clean_expired_links()
    cursor.execute('SELECT * FROM cars')
    rows = cursor.fetchall()
    for link in links:
        # preventing duplicates
        link = 'https://bama.ir'+link.get('href')
        count = 0
        for row in rows:
            if link == row[1]:
                count += 1
        if count > 0:
            continue
    
        # scraping values
        row = link
        try:
            res = requests.get(row,headers=headers)
        except requests.RequestException as e:
            logging.warning(f"Request failed for {link[1]}: {e}")
        if res.status_code == 200:
            logging.info("Connected successfully")
            soup = BeautifulSoup(res.text,'html.parser')
            model = None
            engine = None
            for span in soup.find_all('span',class_='bama-ad-detail-title__subtitle'):
                if span.text.strip().isdigit():
                    model = span.text.strip()
                else: engine = span.text.strip()
            
            for span in soup.find_all('span',class_='address-text'):
                location = span.text
        
            for p in soup.find_all('p',class_='dir-ltr'):
                km = p.text
            
            data = (soup.find_all('p'))
            for i in range(len(data)):
                data[i] = data[i].text.split('\n')
            bodystat, color = data[3][0], data[4][0]

            price = (soup.find_all('span',class_='bama-ad-detail-price__price-text'))
            price = (price[0].text.strip())
        else:
            logging.error("Failed to connect to %s", row)
            continue

        i = get_next_id()

        # inserting values into the database
        qury = 'INSERT INTO cars VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        val = (i,link,model,engine,location,km,bodystat,color,price)
        cursor.execute(qury,val) 

    reindexing_ids()

    cnx.commit()
    cnx.close()
    cursor.close()

if __name__ == "__main__":
    main()