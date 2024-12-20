import requests
from bs4 import BeautifulSoup
from google.cloud import firestore
from datetime import datetime, timedelta
import re
import time

# Initialize Firestore
db = firestore.Client.from_service_account_json("serviceAccountKey.json")
school_doc_ref = db.collection("schools").document("university_of_michigan")

def parse_post_date(datetime_str):
    try:
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def extract_author(author_tag):
    if not author_tag:
        return "Anonymous"
    author_text = author_tag.get_text(strip=True)
    author = re.sub(r"^by:\s*", "", author_text, flags=re.IGNORECASE)
    return author.strip() if author else "Anonymous"

def scrape_post_details(post_url):
    """Scrape the full post and all comments across multiple pages."""
    comments = []
    main_post_content = None
    first_page = True

    # Extract base URL (without trailing slash)
    if post_url.endswith('/'):
        base_post_url = post_url[:-1]
    else:
        base_post_url = post_url

    page_number = 1
    print(f"Scraping details for post: {post_url}")  # Debug line

    while True:
        if page_number == 1:
            current_url = base_post_url + '/'
        else:
            current_url = base_post_url + f'/page-{page_number}/'

        print(f"Fetching post details from: {current_url}")  # Debug line
        response = requests.get(current_url)
        if response.status_code != 200:
            print(f"Received status code {response.status_code} for {current_url}, ending pagination.")  # Debug line
            break

        page_soup = BeautifulSoup(response.text, "html.parser")

        if first_page:
            # Extract main post content
            main_post_box = page_soup.select_one(".latest-discussion > .discussion-box.clearfix")
            if main_post_box:
                # If multiple paragraphs in main post, join them with double newlines
                paragraphs = main_post_box.select(".discussion-box-content p")
                if paragraphs:
                    # Use get_text() without strip to preserve spacing
                    full_text = "\n\n".join(p.get_text() for p in paragraphs)
                    main_post_content = full_text.strip()  # strip trailing spaces if needed
                else:
                    main_post_content = "No content"
            else:
                print("No main post content found on first page.")
            first_page = False

        # Track comment count for this page
        page_comments_count = 0

        # Extract comments from this page
        reply_boxes = page_soup.select(".discussion-box-reply .discussion-box.clearfix")
        if not reply_boxes:
            print("No comments found on this page.")  # Debug line

        for reply in reply_boxes:
            time_tag = reply.select_one(".posted-date time")
            comment_date = parse_post_date(time_tag['datetime']) if time_tag and time_tag.has_attr('datetime') else None

            # Author
            author_tag = reply.select_one(".comment") or reply.select_one(".discussion-box-head span span")
            author = extract_author(author_tag)

            # Comment content
            paragraphs = reply.select(".discussion-box-content p")
            if paragraphs:
                comment_text = "\n\n".join(p.get_text() for p in paragraphs).strip()
            else:
                comment_text = "No content"

            comments.append({
                "author": author,
                "date": comment_date.strftime("%Y-%m-%d %H:%M:%S") if comment_date else None,
                "content": comment_text
            })
            page_comments_count += 1

        print(f"Extracted {page_comments_count} comments from {current_url}")  # Debug line

        # Check if NEXT > link exists on this page
        pagination_links = page_soup.select(".post-pagination-list li a")
        next_link = None
        for link in pagination_links:
            if "NEXT" in link.get_text(strip=True).upper():
                next_link = link
                break

        if next_link:
            page_number += 1
            time.sleep(1)
        else:
            # No next page
            print("No NEXT link found, finished scraping this post's comments.")  # Debug line
            break

    print(f"Total comments for post {post_url}: {len(comments)}\n")  # Debug line
    return main_post_content, comments

def upload_single_post_to_firestore(post):
    # Writes a single post document to Firestore
    try:
        print(f"Uploading post '{post['title']}' immediately to Firestore...")
        school_doc_ref.collection("posts").add(post)
        print(f"Uploaded post: {post['title']} with {len(post['comments'])} comments")
    except Exception as e:
        print(f"Error uploading post '{post['title']}': {e}")

def scrape_greekrank_posts():
    base_url = "https://www.greekrank.com/uni/62/discussion/"
    two_weeks_ago = datetime.now() - timedelta(weeks=2)
    posts = []
    page = 1
    reached_old_posts = False  # To track when we encounter older posts

    while True:
        # Construct URL for current page
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}page-{page}/"

        print(f"Fetching discussion listings from: {url}")  # Debug line
        response = requests.get(url)
        if response.status_code != 200:
            print("No valid response, stopping main pagination.")
            break

        soup = BeautifulSoup(response.text, "html.parser")

        # If no discussion boxes found, break (no more pages)
        discussion_boxes = soup.select(".discussion-box.clearfix")
        if not discussion_boxes:
            print("No discussion boxes found. Stopping main pagination.")
            break

        for post_element in discussion_boxes:
            # Extract the title and post URL
            title_tag = post_element.select_one("h5.discussion-box-head a")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"
            post_url = title_tag['href'] if title_tag and title_tag.has_attr('href') else None
            if post_url and post_url.startswith("/"):
                post_url = "https://www.greekrank.com" + post_url

            # Extract snippet (fallback content)
            content_tag = post_element.select_one(".discussion-box-content p")
            snippet_content = content_tag.get_text() if content_tag else "No content"
            snippet_content = snippet_content.strip()

            # Extract author
            author_tag = post_element.select_one(".comment")
            author = extract_author(author_tag)

            # Extract date/time
            time_tag = post_element.select_one(".posted-date time")
            post_date = parse_post_date(time_tag['datetime']) if time_tag and time_tag.has_attr('datetime') else None

            # Check timeframe
            if post_date and post_date >= two_weeks_ago and post_url:
                print(f"Scraping post: {title} ({post_url})")  # Debug line
                full_content, comments = scrape_post_details(post_url)
                if not full_content:
                    full_content = snippet_content

                new_post = {
                    "title": title,
                    "content": full_content,
                    "author": author,
                    "date": post_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "comments": comments
                }

                print(f"Adding post '{title}' with {len(comments)} comments.")
                posts.append(new_post)

                # Write as we go:
                upload_single_post_to_firestore(new_post)

            else:
                # We've encountered a post older than two weeks or invalid
                print(f"Skipping post '{title}' (No recent date or invalid URL). Older post encountered.")
                reached_old_posts = True
                break

        if reached_old_posts:
            print("Reached older posts, stopping scraping and we have already uploaded recent posts.")
            break

        # Check if there's a next page link on the discussion listing
        pagination_links = soup.select(".post-pagination-list li a")
        next_link = None
        for link in pagination_links:
            if "NEXT" in link.get_text(strip=True).upper():
                next_link = link
                break

        if next_link:
            page += 1
            time.sleep(1)
        else:
            # No next page link, stop scraping
            print("No NEXT link for discussion pages, stopping main scrape.")
            break

    return posts

if __name__ == "__main__":
    print("Scraping posts from GreekRank for University of Michigan...")
    posts = scrape_greekrank_posts()
    print(f"Found {len(posts)} posts from the last 2 weeks.")