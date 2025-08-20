link = "/wow-classic-hardcore-boost/self-found-powerleveling"



def process_nestedlink(nestedlink, row, category):
    driver = webdriver.Edge(options=options)
    result = {}
    try:
        if nestedlink.startswith("/"):
            nestedlink = "https://skycoach.gg" + nestedlink
        print(nestedlink)
        driver.get(nestedlink)
        time.sleep(2)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        return
        # name_tag = soup.find("div", class_="game-header")
        # name = None
        # if name_tag:
        #     h1 = name_tag.find("h1")
        #     if h1:
        #         name = h1.get_text(strip=True)
        # desc = None
        # desc_section = soup.find("div", class_="product-info-section__html")
        # if desc_section:
        #     desc = desc_section.get_text(separator="\n", strip=True)
        # icon = None
        # picture = soup.find("picture", class_="responsive-image")
        # if picture:
        #     img = picture.findAll("img")
        #     # (old code, not needed for icon extraction)
        # price = None
        # price_span = soup.find("span", class_="payment-summary__price-column-total")
        # if price_span:
        #     price = price_span.get_text(strip=True)
        # cat = row.get("Category", category)

        # # Extract game character image
        # game_char_image = None
        # image_container = soup.find("div", class_="offer-card__image-container")
        # if image_container:
        #     picture_tag = image_container.find("picture", class_="responsive-image offer-card__image")
        #     if picture_tag:
        #         # Try to get src from <source> first, then <img>
        #         src = None
        #         source_tag = picture_tag.find("source")
        #         img_tag = picture_tag.find("img")
        #         if source_tag and source_tag.has_attr("srcset"):
        #             src = source_tag["srcset"]
        #         elif img_tag and img_tag.has_attr("src"):
        #             src = img_tag["src"]
        #         if src and src.startswith("/"):
        #             src = "https://skycoach.gg" + src
        #         game_char_image = src

        # result = {
        #     "Name": name,
        #     "Description": desc,
        #     "Icon": icon,
        #     "Price": price,
        #     "Category": cat,
        #     "Link": row['Link'],
        #     "GameCharImage": game_char_image
        # }
    finally:
        driver.quit()
    return result


process_nestedlink(link, {}, "World of Warcraft Classic Hardcore Boost")