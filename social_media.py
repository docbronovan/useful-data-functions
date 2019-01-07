# social_media.py

#################
# # INSTAGRAM # #
#################
""" Scrape data from frontpage of instagram account"""
import time
from bs4 import BeautifulSoup
import urllib
import json
import pandas as pd
r = urllib.urlopen('https://www.instagram.com/catsonsynthesizersinspace/').read()
soup = BeautifulSoup(r, "lxml")
js_data = soup.find_all("script", type="text/javascript")
post_data = js_data[3]
post_json = json.loads(post_data.text.replace("window._sharedData = ", '')[:-1])
entry_data = post_json['entry_data']['ProfilePage'][0]['graphql']['user']
media = entry_data['edge_owner_to_timeline_media']['edges']
column_names = ['code', 'gating_info', 'dimensions', 'caption', 'comments_disabled', '__typename',
            'comments', 'date', 'media_preview', 'likes', 'owner', 'thumbnail_src', 'is_video', 'id',
            'display_src']

df_final = pd.DataFrame({})
for posts in media:
    post_dict = dict(posts)['node']
    post_dict['code'] = post_dict['shortcode']
    post_dict['gating_info'] = ''
    post_dict['dimensions'] = ''
    if isinstance(post_dict['edge_media_to_caption'], dict):
        post_dict['caption'] = post_dict['edge_media_to_caption']['edges'][0]['node']['text']
    post_dict['comments_disabled'] = str(post_dict['comments_disabled'])
    # __typename is fine
    if isinstance(post_dict['edge_media_to_comment'], dict):
        post_dict['comments'] = post_dict['edge_media_to_comment']['count']
    post_dict['date'] = post_dict['taken_at_timestamp']
    # media_preview is fine
    if isinstance(post_dict['edge_media_preview_like'], dict):
        post_dict['likes'] = post_dict['edge_media_preview_like']['count']
    if isinstance(post_dict['owner'], dict):
        post_dict['owner'] = post_dict['owner']['id']
    post_dict['thumbnail_src'] = post_dict['thumbnail_src']
    # id is fine
    if not isinstance(post_dict['thumbnail_resources'], dict):
        post_dict['thumbnail_resources'] = ''
    post_dict['display_src'] = post_dict['display_url']
    post_trimmed = {k: post_dict[k] for k in column_names}
    posts_df = pd.DataFrame(post_trimmed, index=[0])

    # create url data piece from 'code' piece
    posts_df['url'] = "https://www.instagram.com/p/" + posts_df['code']

    # convert time
    time_converter = lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x))
    posts_df["date"] = posts_df['date'].apply(time_converter)

    # rename columns
    posts_df.rename(columns={'date': 'date_time'}, inplace=True)
    posts_df.rename(columns={'__typename': 'typename'}, inplace=True)
    posts_df.rename(columns={'id': 'ig_id'}, inplace=True)
    posts_df['caption'] = posts_df['caption'].str.replace('\'', '\'\'')
    df_final = pd.concat([df_final,posts_df])

print(df_final)
