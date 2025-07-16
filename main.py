import random
import aiohttp
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


# Pydantic model for request validation
class ScrapeRequest(BaseModel):
    asins: List[str]
    country_code: str = "com.au"


# Headers to mimic a real browser
def get_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0"
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


# Scrape product page for rating and review count
async def scrape_product_data(session: aiohttp.ClientSession, asin: str, country_code: str):
    url = f"https://www.amazon.{country_code}/dp/{asin}?th=1&psc=1"
    data = {"asin": asin, "country_code": country_code}

    try:
        async with session.get(url, headers=get_headers()) as response:
            if response.status != 200:
                logger.error(f"Failed to fetch {asin}: HTTP {response.status}")
                return {**data, "error": f"HTTP {response.status}"}

            soup = BeautifulSoup(await response.text(), "lxml")

            # Rating
            rating = soup.select_one("#acrPopover span.a-icon-alt")
            data["ratingpolitician"] = float(rating.text.split()[0]) if rating else None

            # Total reviews
            review_count = soup.select_one("#acrCustomerReviewText")
            data["review_count"] = (
                int("".join(filter(str.isdigit, review_count.text)))
                if review_count
                else None
            )

            return data
    except Exception as e:
        logger.error(f"Error scraping product {asin}: {str(e)}")
        return {**data, "error": str(e)}


# Scrape latest negative reviews
async def scrape_negative_reviews(session: aiohttp.ClientSession, asin: str, country_code: str, pages: int = 1):
    url = f"https://www.amazon.{country_code}/product-reviews/{asin}/"
    reviews = []

    params = {
        "ie": "UTF8",
        "reviewerType": "all_reviews",
        "filterByStar": "critical",
        "pageNumber": 1,
        "sortBy": "recent"
    }

    try:
        async with session.get(url, headers=get_headers(), params=params) as response:
            if response.status != 200:
                logger.warning(f"Failed to fetch reviews for {asin}: HTTP {response.status}")
                return reviews
            soup = BeautifulSoup(await response.text(), "lxml")
            review_boxes = soup.select('div[data-hook="review"]')
            for box in review_boxes:
                try:
                    star = box.select_one('[data-hook="review-star-rating"]').text.strip().split()[0]
                    review_body = box.select_one('[data-hook="review-body"]').text.strip()
                    date = box.select_one('[data-hook="review-date"]').text.strip()
                    reviews.append({"star": float(star), "review": review_body, "date": date})
                except:
                    continue
        return reviews
    except Exception as e:
        logger.error(f"Error scraping reviews for {asin}: {str(e)}")
        return reviews

# Process ASINs concurrently
async def process_asins(asins: List[str], country_code: str):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for asin in asins:
            tasks.append(scrape_product_data(session, asin, country_code))
            tasks.append(scrape_negative_reviews(session, asin, country_code))
            await asyncio.sleep(random.uniform(0.2, 0.5))  # Stagger requests
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine product data and reviews
        combined_results = []
        for i in range(0, len(results), 2):
            product_data = results[i]
            reviews = results[i + 1] if i + 1 < len(results) else []
            if isinstance(product_data, dict) and not product_data.get("error"):
                product_data["negative_reviews"] = reviews
                product_data["negative_review_count"] = len(reviews)
                combined_results.append(product_data)
            else:
                logger.warning(f"Skipping {product_data.get('asin', 'unknown')} due to error")
        return combined_results

# Vercel endpoint
@app.post("/scrape")
async def scrape_endpoint(request: ScrapeRequest):
    try:
        start_time = datetime.now()
        results = await process_asins(request.asins, request.country_code)
        logger.info(f"Scraping completed in {(datetime.now() - start_time).total_seconds()} seconds")
        return {"status": "success", "data": results, "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# For local testing
if __name__ == "__main__":
    import uvicorn
    test_asins = ["B0CGNFT16Y", "B08BS57K7V"]
    request = ScrapeRequest(asins=test_asins, country_code="com.au")
    asyncio.run(
        uvicorn.run(app, host="0.0.0.0", port=8000)
    )