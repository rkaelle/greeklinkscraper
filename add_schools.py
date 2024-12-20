import re
import requests
from bs4 import BeautifulSoup
from google.cloud import firestore

# Initialize Firestore
db = firestore.Client.from_service_account_json("serviceAccountKey.json")

def slugify_name(name: str) -> str:
    # Convert "University of Michigan" -> "university_of_michigan"
    return re.sub(r'[^a-z0-9]+', '_', name.strip().lower()).strip('_')

def fetch_schools(url: str):
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Select all <a> tags with the style attribute containing font-weight:600; 
    # This matches the pattern from the provided snippet.
    school_links = soup.select('div.inner-container.clearfix.discussions-section div[style*="padding:10px"] a[style*="font-weight:600;"]')

    schools = []
    for link in school_links:
        school_name = link.get_text(strip=True)
        # slugify main school name for discussion page URL (omit abbreviations after '-')
        main_name = school_name.split('-')[0].strip()
        slug = slugify_name(main_name)
        discussion_url = f"/discussions?school={slug}"

        schools.append({
            "name": school_name,
            "discussionPageUrl": discussion_url
        })
    return schools

def add_schools_to_firestore(schools):
    for school in schools:
        doc_id = slugify_name(school["name"].split('-')[0].strip())
        doc_ref = db.collection("schools").document(doc_id)
        doc_ref.set({
            "name": school["name"],
            "discussionPageUrl": school["discussionPageUrl"]
        })
        print(f"Added/Updated school: {school['name']}")

if __name__ == "__main__":
    url = "https://www.greekrank.com/list/"
    schools_list = fetch_schools(url)
    add_schools_to_firestore(schools_list)
    print("All schools have been added/updated successfully.")