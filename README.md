# DD2476-project

Yandex Personalized Web Search Challange: https://www.kaggle.com/c/yandex-personalized-web-search-challenge

## Database schema:

1. site2domain(site, domain)
   * site -> domain
2. session(session_id, day, user)
   * session_id -> day, user
3. query(query_id, query)
   * query_id -> query (the query text)
   * query_id is the id of a query text
4. serp(session_id, serp, time_passed, query_type, query_id, site[0-9])
   * session_id, serp -> time_passed, query_type, query_id, site[0-9]
   * site[0-9] = site0, site1, site2, ..., The order matters because it is the order they appear in the serp, site0 has a higher unpersonalized rank that site1...
   * query_id doesn't give us the sites because the query results depends on the time the query was issued (crawlers changes the results.
5. click(session_id, serp, time_passed, site)
   * session_id, serp, time_passed -> site
