import os
import sys
import argparse
from dotenv import load_dotenv
from notion_client import Client


def load_environment():
    """Load environment variables and verify required variables exist."""
    load_dotenv()

    required_vars = ["NOTION_API_KEY", "MASTER_DB_ID", "SLAVE_DB_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Error: The following required environment variables are missing: {', '.join(missing_vars)}")
        print("Please add them to your .env file")
        sys.exit(1)

    return {
        "api_key": os.getenv("NOTION_API_KEY"),
        "master_db_id": os.getenv("MASTER_DB_ID"),
        "slave_db_id": os.getenv("SLAVE_DB_ID"),
    }


def get_master_pages(notion, db_id, limit: int = None):
    """Retrieve pages from master database that need syncing."""
    query = {
        "filter": {
            "and": [
                {"property": "Sync Status", "select": {"equals": "Not Synced"}},
                {"property": "Sync?", "select": {"equals": "True"}},
            ]
        }
    }

    if limit is not None:
        query["page_size"] = min(limit, 100)

    # Initial pagination
    response = notion.databases.query(database_id=db_id, **query)
    pages = response.get("results", [])

    # Continue pagination if needed
    while response.get("has_more", False) and (limit is None or len(pages) < limit):
        query["start_cursor"] = response.get("next_cursor")
        response = notion.databases.query(database_id=db_id, **query)
        pages.extend(response.get("results", []))

        if limit is not None and len(pages) >= limit:
            pages = pages[:limit]
            break

    print(f"Total pages retrieved: {len(pages)}")

    return pages


def extract_property_value(page, property_name):
    """Extract the value of a property from a Notion page."""
    if property_name not in page["properties"]:
        return None

    prop = page["properties"][property_name]
    prop_type = prop["type"]

    if prop_type == "title":
        title_items = prop.get("title", [])
        if not title_items:
            return ""
        # Join all title segments into a single string
        return "".join(item["text"]["content"] for item in title_items)
    elif prop_type == "rich_text":
        rich_text_items = prop.get("rich_text", [])
        if not rich_text_items:
            return ""
        # Join all rich text segments into a single string
        return "".join(item["text"]["content"] for item in rich_text_items)
    elif prop_type == "number":
        return prop.get("number")
    elif prop_type == "select":
        select_data = prop.get("select")
        return select_data.get("name") if select_data else None
    elif prop_type == "multi_select":
        return [item["name"] for item in prop.get("multi_select", [])]
    elif prop_type == "date":
        date_data = prop.get("date")
        return date_data.get("start") if date_data else None
    elif prop_type == "url":
        return prop.get("url")
    elif prop_type == "email":
        return prop.get("email")
    elif prop_type == "phone_number":
        return prop.get("phone_number")
    elif prop_type == "checkbox":
        return prop.get("checkbox")
    elif prop_type == "formula":
        formula_data = prop.get("formula", {})
        if "string" in formula_data:
            return formula_data.get("string")
        elif "number" in formula_data:
            return formula_data.get("number")
        elif "boolean" in formula_data:
            return formula_data.get("boolean")
        elif "date" in formula_data:
            date_data = formula_data.get("date")
            return date_data.get("start") if date_data else None
    else:
        return None


def create_property_object(property_name, value, property_type):
    """Create a property object for the new page based on property type."""
    if property_type == "title":
        return {"title": [{"type": "text", "text": {"content": str(value) if value else ""}}]}
    elif property_type == "rich_text":
        return {"rich_text": [{"type": "text", "text": {"content": str(value) if value else ""}}]}
    elif property_type == "number":
        return {"number": value if value is not None else None}
    elif property_type == "select":
        return {"select": {"name": value}} if value else {"select": None}
    elif property_type == "multi_select":
        return {"multi_select": [{"name": item} for item in value]} if value else {"multi_select": []}
    elif property_type == "date":
        return {"date": {"start": value}} if value else {"date": None}
    elif property_type == "url":
        return {"url": value if value else None}
    elif property_type == "checkbox":
        return {"checkbox": value if value is not None else False}
    else:
        return None


def map_property_types(notion, db_id):
    """Map property names to their types for a database."""
    db_info = notion.databases.retrieve(database_id=db_id)
    return {name: prop["type"] for name, prop in db_info["properties"].items()}


def sync_page(notion, master_page, slave_db_id, property_types):
    """Create a matching page in the slave database."""
    properties_to_sync = [
        "Name",
        "Impressions",
        "Likes",
        "Bookmarks",
        "Retweets",
        "Comments",
        "CTR",
        "URL",
        "Author",
        "Handle",
        "Date",
        "Retention",
        "Engagement Rate",
        "Niche",
    ]

    new_properties = {}
    missing_properties = []

    # Check all properties first to ensure they have data
    for prop_name in properties_to_sync:
        if prop_name not in master_page["properties"]:
            missing_properties.append(prop_name)
            continue

        value = extract_property_value(master_page, prop_name)
        if value is None or value == "" or (isinstance(value, list) and len(value) == 0):
            missing_properties.append(prop_name)
            continue

        # If we get here, the property has valid data
        if prop_name == "Name" and "Name" in property_types and property_types["Name"] == "title":
            new_properties["Name"] = create_property_object("Name", value, "title")
        elif prop_name in property_types:
            new_properties[prop_name] = create_property_object(prop_name, value, property_types[prop_name])

    # If any required properties are missing, raise an exception
    if missing_properties:
        raise ValueError(f"Missing or empty required properties: {', '.join(missing_properties)}")

    # Create the new page in the slave database
    new_page = notion.pages.create(parent={"database_id": slave_db_id}, properties=new_properties)

    return new_page


def update_sync_status(notion, page_id, status="Synced"):
    """Update the Sync Status property.

    Args:
        notion: Notion client
        page_id: ID of the page to update
        status: Status to set ("Synced" or "Failed")
    """
    notion.pages.update(page_id=page_id, properties={"Sync Status": {"select": {"name": status}}})


def main():
    parser = argparse.ArgumentParser(description="Sync Notion databases")
    parser.add_argument("--limit", type=int, help="Number of pages to sync (default: all)", default=None)
    args = parser.parse_args()

    # Load environment variables
    env = load_environment()

    # Initialize Notion client
    notion = Client(auth=env["api_key"])

    # Get property types for slave database
    slave_property_types = map_property_types(notion, env["slave_db_id"])

    # Get pages from master database that need syncing
    pages_to_sync = get_master_pages(notion, env["master_db_id"], args.limit)

    if not pages_to_sync:
        print("No pages need to be synced.")
        return

    print(f"Found {len(pages_to_sync)} pages to sync.")

    # Sync each page
    for i, page in enumerate(pages_to_sync, 1):
        try:
            page_name = extract_property_value(page, "Name") or f"Page {i}"
            print(f"Syncing page {i}/{len(pages_to_sync)}: {page_name}")

            # Create matching page in slave database
            new_page = sync_page(notion, page, env["slave_db_id"], slave_property_types)

            # Update sync status in master database
            update_sync_status(notion, page["id"])

            print(f"Successfully synced page.")
        except ValueError as e:
            # Specifically handle missing properties error
            print(f"Failed to sync page - {str(e)}")
            update_sync_status(notion, page["id"], status="Failed")
            print(f"Marked page as Failed and continuing with next page.")
        except Exception as e:
            # Handle other errors
            print(f"Error syncing page: {str(e)}")
            update_sync_status(notion, page["id"], status="Failed")
            print(f"Marked page as Failed and continuing with next page.")

    print(f"Sync completed. {len(pages_to_sync)} pages processed.")


if __name__ == "__main__":
    main()
