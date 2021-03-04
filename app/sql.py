from bot import *


def rating_messages():
    content = (ContentModel.select(ContentModel.id).
               where((ContentModel.type != 'text') &
                     (ContentModel.reply.is_null())).
               order_by(-ContentModel.time).
               limit(500)).alias('content')
    shows = (ShowsModel.select(ShowsModel.id, ShowsModel.content, ShowsModel.reaction).where(
        ShowsModel.content.in_(content)))

    count = fn.COUNT(ShowsModel.id).alias('count')
    reactions = (shows.select(ShowsModel.content, ShowsModel.reaction, count).
                 group_by(ShowsModel.content, ShowsModel.reaction))

    rnull_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction.is_null())).alias('r_null')
    r1_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 1)).alias('r1')
    r2_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 2)).alias('r2')
    r3_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 3)).alias('r3')
    r4_shows = (reactions.select(ShowsModel.content, count).where(ShowsModel.reaction == 4)).alias('r4')
    content_values = (
        ContentModel.select(ContentModel.id,
                            fn.COALESCE(r1_shows.c.count, 0).alias('r1_count'),
                            fn.COALESCE(r2_shows.c.count, 0).alias('r2_count'),
                            fn.COALESCE(r3_shows.c.count, 0).alias('r3_count'),
                            fn.COALESCE(r4_shows.c.count, 0).alias('r4_count'),
                            fn.COALESCE(rnull_shows.c.count, 0).alias('rnull_count')).
            join(content, on=(content.c.id == ContentModel.id)).
            join(r1_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r1_shows.c.content_id)).
            join(r2_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r2_shows.c.content_id)).
            join(r3_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r3_shows.c.content_id)).
            join(r4_shows, JOIN.FULL_OUTER, on=(ContentModel.id == r4_shows.c.content_id)).
            join(rnull_shows, JOIN.FULL_OUTER, on=(ContentModel.id == rnull_shows.c.content_id))
    ).alias('content_values')

    metric = (
            content_values.c.r1_count * (-5) +
            content_values.c.r2_count * (-3) +
            content_values.c.r3_count * (-1) +
            content_values.c.r4_count * (+1)
    )
    top_metric = (ContentModel.select(ContentModel, metric.alias('metric')).
                  join(content_values, on=(content_values.c.id == ContentModel.id)).
                  order_by(-metric).
                  limit(100))

    query = (ContentModel.select(ContentModel, top_metric.c.metric).
             join(top_metric, on=(top_metric.c.id == ContentModel.id)))


rating_messages()
