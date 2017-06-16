import scala.util.parsing.json._

println("Hello world")

val x = """{"entities": {"hashtags": [], "symbols": [], "user_mentions": [], "urls": [{"expanded_url": "https://twitter.com/lasprovincias/status/852212224848482304", "display_url": "twitter.com/lasprovincias/\u2026", "url": "https://t.co/mtUf3TNpep", "indices": [71, 94]}]}, "text": "Son las malas personas las que hacen que la pol\u00edtica sea repugnante... https://t.co/mtUf3TNpep", "truncated": false, "id": 852220119619162114, "retweet_count": 0, "in_reply_to_user_id": null, "quoted_status_id_str": "852212224848482304", "source": "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>", "place": null, "in_reply_to_status_id": null, "in_reply_to_screen_name": null, "favorited": false, "quoted_status": {"in_reply_to_status_id": null, "entities": {"hashtags": [], "symbols": [], "user_mentions": [], "urls": [{"expanded_url": "http://www.lasprovincias.es/politica/201704/12/jordi-pujol-hijo-oculto-20170412132830-rc.html", "display_url": "lasprovincias.es/politica/20170\u2026", "url": "https://t.co/WUTWD8v71n", "indices": [77, 100]}]}, "in_reply_to_screen_name": null, "truncated": false, "id": 852212224848482304, "retweet_count": 0, "in_reply_to_user_id": null, "coordinates": null, "place": null, "id_str": "852212224848482304", "user": {"utc_offset": 7200, "screen_name": "lasprovincias", "follow_request_sent": null, "listed_count": 1719, "statuses_count": 108795, "id": 17618159, "profile_background_color": "FFFFFF", "followers_count": 142638, "favourites_count": 55, "is_translator": false, "id_str": "17618159", "friends_count": 170, "profile_background_image_url_https": "https://pbs.twimg.com/profile_background_images/549279100/fondoTwitter.jpg", "profile_background_image_url": "http://pbs.twimg.com/profile_background_images/549279100/fondoTwitter.jpg", "following": null, "location": "Valencia, Spain", "time_zone": "Madrid", "profile_sidebar_fill_color": "EFEFEF", "profile_image_url": "http://pbs.twimg.com/profile_images/678867012655910912/r7K-rHSq_normal.jpg", "url": "http://www.lasprovincias.es", "notifications": null, "profile_image_url_https": "https://pbs.twimg.com/profile_images/678867012655910912/r7K-rHSq_normal.jpg", "profile_banner_url": "https://pbs.twimg.com/profile_banners/17618159/1491473263", "created_at": "Tue Nov 25 12:22:35 +0000 2008", "geo_enabled": true, "contributors_enabled": false, "description": "Noticias de la Comunitat Valenciana, de Espa\u00f1a y de \u00e1mbito internacional WhatsApp: 626641201. Agr\u00e9ganos y env\u00edanos tu nombre. https://telegram.me/lasprovincias", "name": "LAS PROVINCIAS", "profile_background_tile": false, "profile_text_color": "333333", "verified": true, "lang": "es", "default_profile": false, "default_profile_image": false, "profile_sidebar_border_color": "FFFFFF", "profile_use_background_image": false, "protected": false, "profile_link_color": "009999"}, "favorited": false, "filter_level": "low", "text": "Jordi Pujol hijo ocult\u00f3 14 millones desde que se le investiga, seg\u00fan la UDEF https://t.co/WUTWD8v71n", "possibly_sensitive": false, "created_at": "Wed Apr 12 17:30:02 +0000 2017", "in_reply_to_user_id_str": null, "is_quote_status": false, "source": "<a href=\"https://about.twitter.com/products/tweetdeck\" rel=\"nofollow\">TweetDeck</a>", "favorite_count": 0, "lang": "es", "contributors": null, "geo": null, "in_reply_to_status_id_str": null, "retweeted": false}, "filter_level": "low", "display_text_range": [0, 70], "user": {"utc_offset": null, "screen_name": "VicenteLizondo", "follow_request_sent": null, "listed_count": 12, "statuses_count": 1462, "id": 2882688729, "profile_background_color": "000000", "followers_count": 1450, "favourites_count": 698, "is_translator": false, "id_str": "2882688729", "friends_count": 8, "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png", "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png", "following": null, "location": "Valencia", "time_zone": null, "profile_sidebar_fill_color": "000000", "profile_image_url": "http://pbs.twimg.com/profile_images/822527706634092544/oTjCfNn9_normal.jpg", "url": "http://www.vglizondo.org", "notifications": null, "profile_image_url_https": "https://pbs.twimg.com/profile_images/822527706634092544/oTjCfNn9_normal.jpg", "profile_banner_url": "https://pbs.twimg.com/profile_banners/2882688729/1463770863", "created_at": "Tue Nov 18 15:12:17 +0000 2014", "geo_enabled": false, "contributors_enabled": false, "description": "Valenciano, Espa\u00f1ol y Europeo. Los Valencian@s NO nos merecemos estos pol\u00edticos.", "name": "Vicente Lizondo", "profile_background_tile": false, "profile_text_color": "000000", "verified": false, "lang": "es", "default_profile": false, "default_profile_image": false, "profile_sidebar_border_color": "000000", "profile_use_background_image": false, "protected": false, "profile_link_color": "FA743E"}, "created_at": "Wed Apr 12 18:01:24 +0000 2017", "id_str": "852220119619162114", "in_reply_to_user_id_str": null, "possibly_sensitive": false, "coordinates": null, "favorite_count": 0, "lang": "es", "contributors": null, "timestamp_ms": "1492020084658", "geo": null, "is_quote_status": true, "retweeted": false, "in_reply_to_status_id_str": null, "quoted_status_id": 852212224848482304}"""


def findVal(str: String, ToFind: String): String = {
  JSON.parseFull(str) match {
    case Some(m: Map[String, String]) => m(ToFind)
  }
}

(findVal(x, "text"), findVal(x, "lang"))



def getTweetsAndLang(input: String): (String, Int) = {
  try {
    var result = (findVal(input, "text"), -1)

    if (findVal(input, "lang") == "en") result.copy(_2 = 0)
    else if (findVal(input, "lang") == "es") result.copy(_2 = 1)
    else result
  } catch {
    case e: Exception => ("unknown", -1)
  }
}

getTweetsAndLang(x)

