import re
import requests
import time
from bs4 import BeautifulSoup
from google.cloud import firestore
from datetime import datetime, timedelta, timezone

# ---------------------- Shared Initialization ----------------------
db = firestore.Client.from_service_account_json("serviceAccountKey.json")

def slugify_name(name: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', name.strip().lower()).strip('_')

# ---------------------- Part 1: Fetch & Store Schools ----------------------
def fetch_schools(url: str):
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Select all <a> tags with the style attribute containing font-weight:600; 
    school_links = soup.select('div.inner-container.clearfix.discussions-section div[style*="padding:10px"] a[style*="font-weight:600;"]')

    schools = []
    for link in school_links:
        school_name = link.get_text(strip=True)
        main_name = school_name.split('-')[0].strip()
        slug = slugify_name(main_name)
        discussion_url = f"/discussions?school={slug}"

        # Extract uni_id from the href attribute
        href = link.get('href', '')
        # Assuming the href contains something like "/uni/62/discussion/"
        uni_id_match = re.search(r'/uni/(\d+)/', href)
        uni_id = uni_id_match.group(1) if uni_id_match else None

        if uni_id:
            schools.append({
                "name": school_name,
                "discussionPageUrl": discussion_url,
                "uni_id": uni_id  # Add uni_id to each school
            })
        else:
            print(f"Warning: uni_id not found for school '{school_name}'. Skipping.")

    return schools

def generate_search_index(name: str) -> list:
    # Split into words
    words = name.lower().split()
    search_index = set()
    
    for word in words:
        # Generate prefixes for each word
        for i in range(1, len(word)+1):
            prefix = word[:i]
            search_index.add(prefix)
    
    return list(search_index)

def add_school_to_firestore(school):
    doc_id = slugify_name(school["name"].split('-')[0].strip())
    lower_name = school["name"].lower()
    search_index = generate_search_index(school["name"])

    doc_ref = db.collection("schools").document(doc_id)
    doc_ref.set({
        "name": school["name"],
        "discussionPageUrl": school["discussionPageUrl"],
        "nameLowerCase": lower_name,
        "searchIndex": search_index,
        "uni_id": school.get("uni_id")  # Store uni_id in Firestore
    })
    print(f"Added/Updated school: {school['name']} with uni_id: {school.get('uni_id')}")

# ---------------------- Part 2: Scrape Posts for All Schools ----------------------
def parse_post_date(datetime_str):
    try:
        # Parse the datetime string and make it timezone-aware (UTC)
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None

def extract_author(author_tag):
    if not author_tag:
        return "Anonymous"
    author_text = author_tag.get_text(strip=True)
    author = re.sub(r"^by:\s*", "", author_text, flags=re.IGNORECASE)
    return author.strip() if author else "Anonymous"

def scrape_post_details(post_url):
    comments = []
    main_post_content = None
    first_page = True

    if post_url.endswith('/'):
        base_post_url = post_url[:-1]
    else:
        base_post_url = post_url

    page_number = 1
    while True:
        if page_number == 1:
            current_url = base_post_url + '/'
        else:
            current_url = base_post_url + f'/page-{page_number}/'

        response = requests.get(current_url)
        if response.status_code != 200:
            break

        page_soup = BeautifulSoup(response.text, "html.parser")

        if first_page:
            main_post_box = page_soup.select_one(".latest-discussion > .discussion-box.clearfix")
            if main_post_box:
                paragraphs = main_post_box.select(".discussion-box-content p")
                if paragraphs:
                    full_text = "\n\n".join(p.get_text() for p in paragraphs)
                    main_post_content = full_text.strip()
                else:
                    main_post_content = "No content"
            first_page = False

        reply_boxes = page_soup.select(".discussion-box-reply .discussion-box.clearfix")
        for reply in reply_boxes:
            time_tag = reply.select_one(".posted-date time")
            comment_date = parse_post_date(time_tag['datetime']) if time_tag and time_tag.has_attr('datetime') else None

            author_tag = reply.select_one(".comment") or reply.select_one(".discussion-box-head span span")
            author = extract_author(author_tag)

            paragraphs = reply.select(".discussion-box-content p")
            if paragraphs:
                comment_text = "\n\n".join(p.get_text() for p in paragraphs).strip()
            else:
                comment_text = "No content"

            comments.append({
                "author": author,
                "date": comment_date,  # Stored as datetime object or None
                "content": comment_text
            })

        # Check if NEXT link exists
        pagination_links = page_soup.select(".post-pagination-list li a")
        next_link = None
        for link in pagination_links:
            if "NEXT" in link.get_text(strip=True).upper():
                next_link = link
                break

        if next_link:
            page_number += 1
            time.sleep(1)  # Be polite and avoid hitting the server too hard
        else:
            break

    return main_post_content, comments

def upload_single_post_to_firestore(post, school_doc_ref):
    try:
        # Assuming each post has a unique title; alternatively, use another unique identifier
        post_id = slugify_name(post['title'])
        post_ref = school_doc_ref.collection("posts").document(post_id)
        post_ref.set(post)
        print(f"Uploaded post: {post['title']} with {len(post['comments'])} comments")
    except Exception as e:
        print(f"Error uploading post '{post['title']}': {e}")

def scrape_greekrank_posts(uni_id, school_doc_ref):
    # Construct base_url using the uni_id
    base_url = f"https://www.greekrank.com/uni/{uni_id}/discussion/"
    two_weeks_ago = datetime.now(timezone.utc) - timedelta(weeks=2)
    page = 1
    reached_old_posts = False

    while True:
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}page-{page}/"

        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to retrieve page {page} for uni_id {uni_id}. Status code: {response.status_code}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        discussion_boxes = soup.select(".discussion-box.clearfix")
        if not discussion_boxes:
            print(f"No discussion boxes found on page {page} for uni_id {uni_id}.")
            break

        for post_element in discussion_boxes:
            title_tag = post_element.select_one("h5.discussion-box-head a")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"
            post_url = title_tag['href'] if title_tag and title_tag.has_attr('href') else None
            if post_url and post_url.startswith("/"):
                post_url = "https://www.greekrank.com" + post_url

            # Extract snippet content
            content_tag = post_element.select_one(".discussion-box-content p")
            snippet_content = content_tag.get_text().strip() if content_tag else "No content"

            # Extract author
            author_tag = post_element.select_one(".comment")
            author = extract_author(author_tag)

            # Extract date
            time_tag = post_element.select_one(".posted-date time")
            post_date = parse_post_date(time_tag['datetime']) if time_tag and time_tag.has_attr('datetime') else None

            # Extract upvotes, downvotes, views from the HTML structure
            like_box = post_element.select_one("ul.like-box")
            if like_box:
                # Upvotes and downvotes
                like_li = like_box.select_one("li.like span")
                unlike_li = like_box.select_one("li.unlike span")

                upvotes = int(like_li.get_text(strip=True)) if like_li else 0
                downvotes = int(unlike_li.get_text(strip=True)) if unlike_li else 0

                # Views
                # The views are typically in the last <li> containing 'Views'
                # For safety, find the li containing 'Views'
                views_li = like_box.find(lambda tag: tag.name == "li" and "Views" in tag.get_text())
                if views_li:
                    views_text = views_li.get_text(strip=True)
                    views_match = re.search(r"(\d+)", views_text)
                    views = int(views_match.group(1)) if views_match else 0
                else:
                    views = 0
            else:
                # Fallback if like-box isn't found (shouldn't happen typically)
                upvotes = 0
                downvotes = 0
                views = 0

            if post_url and post_date and post_date >= two_weeks_ago:
                full_content, comments = scrape_post_details(post_url)
                if not full_content:
                    full_content = snippet_content

                new_post = {
                    "title": title,
                    "content": full_content,
                    "author": author,
                    "date": post_date,  # Stored as datetime object with timezone
                    "comments": comments,
                    "views": views,
                    "upvotes": upvotes,
                    "downvotes": downvotes
                }

                upload_single_post_to_firestore(new_post, school_doc_ref)
            else:
                # We've hit an older or invalid post
                reached_old_posts = True
                break

        if reached_old_posts:
            print(f"Reached posts older than two weeks for uni_id {uni_id}. Stopping.")
            break

        # Check for next page
        pagination_links = soup.select(".post-pagination-list li a")
        next_link = None
        for link in pagination_links:
            if "NEXT" in link.get_text(strip=True).upper():
                next_link = link
                break

        if next_link:
            page += 1
            time.sleep(1)  # Be polite and avoid hitting the server too hard
        else:
            print(f"No more pages to scrape for uni_id {uni_id}.")
            break

# ---------------------- Main Execution ----------------------
if __name__ == "__main__":
    print("Fetching all schools...")
    all_schools = fetch_schools("https://www.greekrank.com/list/")
    
    # Add all schools to Firestore
    for s in all_schools:
        add_school_to_firestore(s)
    
    print("Starting to scrape posts for all schools...")
    
    for school in all_schools:
        uni_id = school.get("uni_id")
        if not uni_id:
            print(f"Skipping school '{school['name']}' due to missing uni_id.")
            continue

        # Define Firestore document reference for the current school
        school_doc_ref = db.collection("schools").document(slugify_name(school["name"].split('-')[0].strip()))
        
        print(f"Scraping posts from GreekRank for {school['name']} (uni_id: {uni_id})...")
        scrape_greekrank_posts(uni_id, school_doc_ref)
        print(f"Finished scraping posts for {school['name']}.\n")
    
    print("All scraping tasks completed.")