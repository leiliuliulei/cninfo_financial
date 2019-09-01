import plotly.express as px


def bar_figure(df, title, stack=False, y_percent=False):

    x = ['-'.join(row) for row in df.index.values]      # 如果是multi-index就拍扁成一个string
    data = [{'x': x, 'y': content, 'type': 'bar', 'name': col_name} for col_name, content in df.iteritems()]

    layout = dict(title=title)

    if stack:
        layout.update(barmode='stack')

    if y_percent:
        layout.update(yaxis=dict(tickformat='.0%'))

    figure = {'data': data, 'layout': layout}

    return figure


def atlas_figure(df, title, y_percent=True):
    fig = px.scatter(data_frame=df, x='PE', y='股东ROE', size='净利润', color='子行业', hover_name='证券简称',
                     size_max=80, opacity=0.7, title=title)

    if y_percent:
        fig.layout.update(yaxis=dict(tickformat='.0%'))

    return fig


def dash_options(a_list):
    return [dict(label=a_item, value=a_item) for a_item in a_list]
