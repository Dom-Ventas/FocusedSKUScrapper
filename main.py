import random
import aiohttp
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import List, Dict, Any, Union

# --- Step 1: Enhanced Logging Configuration ---
# Configure a more detailed logger to get better insights into the scraping process.
# The format includes timestamp, log level, logger name, and the message.
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels of logs

# Add a handler to output logs to the console
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)

# Prevent logs from propagating to the root logger if not needed
logger.propagate = False


app = FastAPI(title="Amazon Scraper API", description="An API to scrape Amazon product data and reviews.")


# --- Step 2: Pydantic Models for Type Safety ---
class ScrapeRequest(BaseModel):
    asins: List[str]
    country_code: str = "com.au"


# --- Step 3: Headers and Proxies ---
# Using a pool of user agents to mimic different browsers and reduce block rate.
def get_headers() -> Dict[str, str]:
    """Returns a dictionary of headers to mimic a real browser request."""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1", # Do Not Track request header
    }


# --- Step 4: Core Scraping Functions ---
async def scrape_product_data(session: aiohttp.ClientSession, asin: str, country_code: str) -> Dict[str, Any]:
    """Scrapes the main product page for rating and total review count."""
    url = f"https://www.amazon.{country_code}/dp/{asin}?th=1&psc=1"
    data = {"asin": asin, "country_code": country_code, "url": url}
    logger.info(f"Scraping product data for ASIN {asin} from URL: {url}")

    try:
        async with session.get(url, headers=get_headers(), timeout=15) as response:
            response_text = await response.text()
            if response.status != 200:
                logger.error(f"Failed to fetch {asin}. Status: {response.status}. URL: {url}")
                # Log the response body to understand what Amazon is sending (e.g., CAPTCHA)
                logger.debug(f"Response body for failed request ({asin}):\n{response_text[:1000]}")
                return {**data, "error": f"HTTP {response.status}"}

            # Check for CAPTCHA in the page title or body, a common anti-scraping technique
            if "captcha" in response_text.lower() or "api-services-support@amazon.com" in response_text.lower():
                logger.warning(f"CAPTCHA detected for ASIN {asin}. URL: {url}")
                return {**data, "error": "CAPTCHA or block page detected"}

            soup = BeautifulSoup(response_text, "lxml")

            # Safely extract rating
            rating_element = soup.select_one("#acrPopover span.a-icon-alt")
            data["rating"] = float(rating_element.text.split()[0]) if rating_element else None

            # Safely extract total review count
            review_count_element = soup.select_one("#acrCustomerReviewText")
            data["review_count"] = (
                int("".join(filter(str.isdigit, review_count_element.text)))
                if review_count_element
                else None
            )
            
            logger.info(f"Successfully scraped product data for {asin}.")
            return data

    except asyncio.TimeoutError:
        logger.error(f"Timeout error scraping product {asin} from {url}")
        return {**data, "error": "Request timed out"}
    except Exception as e:
        logger.error(f"An unexpected error occurred while scraping product {asin}: {e}", exc_info=True)
        return {**data, "error": str(e)}


async def scrape_negative_reviews(session: aiohttp.ClientSession, asin: str, country_code: str) -> List[Dict[str, Any]]:
    """Scrapes the most recent critical reviews for a product."""
    url = f"https://www.amazon.{country_code}/product-reviews/{asin}/"
    reviews: List[Dict[str, Any]] = []
    logger.info(f"Scraping negative reviews for ASIN {asin}")

    params = {
        "ie": "UTF8",
        "reviewerType": "all_reviews",
        "filterByStar": "critical", # 1, 2, and 3-star reviews
        "pageNumber": 1,
        "sortBy": "recent"
    }

    try:
        async with session.get(url, headers=get_headers(), params=params, timeout=15) as response:
            response_text = await response.text()
            if response.status != 200:
                logger.warning(f"Failed to fetch reviews for {asin}. Status: {response.status}. URL: {response.url}")
                logger.debug(f"Response body for failed review request ({asin}):\n{response_text[:1000]}")
                return reviews # Return empty list on failure

            soup = BeautifulSoup(response_text, "lxml")
            review_elements = soup.select('div[data-hook="review"]')
            logger.info(f"Found {len(review_elements)} review elements for {asin}.")

            for box in review_elements:
                try:
                    # Use .get_text() with strip=True for cleaner text extraction
                    star_text = box.select_one('[data-hook="review-star-rating"]').get_text(strip=True)
                    review_body = box.select_one('[data-hook="review-body"]').get_text(strip=True)
                    date_text = box.select_one('[data-hook="review-date"]').get_text(strip=True)
                    
                    reviews.append({
                        "star": float(star_text.split()[0]),
                        "review": review_body,
                        "date": date_text
                    })
                except Exception as e:
                    # Log if a specific review box fails to parse, but continue with others
                    logger.warning(f"Could not parse a review for {asin}. Error: {e}", exc_info=False)
                    continue
        
        logger.info(f"Successfully scraped {len(reviews)} negative reviews for {asin}.")
        return reviews
        
    except asyncio.TimeoutError:
        logger.error(f"Timeout error scraping reviews for {asin} from {url}")
        return reviews
    except Exception as e:
        logger.error(f"An unexpected error occurred while scraping reviews for {asin}: {e}", exc_info=True)
        return reviews


# --- Step 5: Concurrent Processing Logic ---
async def process_asins(asins: List[str], country_code: str) -> List[Dict[str, Any]]:
    """Processes a list of ASINs concurrently, scraping data and reviews."""
    logger.info(f"Starting to process {len(asins)} ASINs for country '{country_code}'.")
    async with aiohttp.ClientSession() as session:
        tasks = []
        for asin in asins:
            # Create tasks for both product data and reviews for each ASIN
            tasks.append(scrape_product_data(session, asin, country_code))
            tasks.append(scrape_negative_reviews(session, asin, country_code))
            # A small, random delay between creating tasks can help avoid rate-limiting
            await asyncio.sleep(random.uniform(0.1, 0.3))
        
        # return_exceptions=True ensures that if one task fails, others can still complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug(f"Raw results from asyncio.gather: {results}")

        # Combine product data and reviews
        combined_results = []
        for i in range(0, len(results), 2):
            product_data = results[i]
            reviews = results[i+1] if i + 1 < len(results) else []

            # Handle exceptions returned by asyncio.gather
            if isinstance(product_data, Exception):
                logger.error(f"Task for product data failed with an exception: {product_data}")
                continue # Skip this ASIN
            if isinstance(reviews, Exception):
                logger.error(f"Task for reviews failed with an exception: {reviews}")
                reviews = [] # Default to empty list if review scraping failed

            # Check for errors captured within the scraping function (e.g., HTTP 500)
            if product_data.get("error"):
                logger.warning(f"Skipping ASIN {product_data.get('asin', 'unknown')} due to error: {product_data['error']}")
            else:
                product_data["negative_reviews"] = reviews
                product_data["negative_review_count"] = len(reviews)
                combined_results.append(product_data)

        logger.info(f"Finished processing. Successfully combined data for {len(combined_results)} ASINs.")
        return combined_results


# --- Step 6: API Endpoints ---
@app.post("/scrape", summary="Scrape Amazon product data for a list of ASINs")
async def scrape_endpoint(request: ScrapeRequest):
    """
    Accepts a list of ASINs and a country code, then scrapes Amazon for product data and recent negative reviews.
    """
    start_time = datetime.now()
    logger.info(f"Received scrape request for ASINs: {request.asins} in country: {request.country_code}")
    
    try:
        results = await process_asins(request.asins, request.country_code)
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Scraping completed in {duration:.2f} seconds.")
        return {
            "status": "success",
            "duration_seconds": duration,
            "data": results,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.critical(f"A critical error occurred in the scrape endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal server error occurred: {str(e)}")


@app.get("/health", summary="Health check endpoint")
async def health_check():
    """Returns a simple 'ok' status to indicate the service is running."""
    logger.info("Health check endpoint was called.")
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# To run this app locally, use: uvicorn your_filename:app --reload
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn server for local development.")
    uvicorn.run(app, host="0.0.0.0", port=8000)
