import dash
import dash_core_components as dcc
import dash_html_components as html
from finance_api import DatabaseAPI, AnalysisAPI
from dash.dependencies import Input, Output
from chart_functions import bar_figure, atlas_figure, dash_options


# 获取股票列表、行业列表、所有公司财务数据
with DatabaseAPI() as db:
    stock_list, industry_list = db.get_stock_list(), db.get_industry_list()

choose_stock = dcc.Dropdown(id='stock-dropdown', options=dash_options(stock_list), value=stock_list[0])
choose_industry = dcc.Dropdown(id='industry-dropdown', options=dash_options(industry_list))
choose_sub_industry = dcc.RadioItems(id='sub_industry-radio')


header = html.H2('财务分析', style={'textAlign': 'center'})

stock_section = html.Div([html.Div('个股'), choose_stock], style={'width': '30%'})
industry_section = html.Div([html.Div('行业'), choose_industry], style={'width': '30%'})
sub_industry_section = html.Div([html.Div('子行业'), choose_sub_industry])

charts = [dcc.Graph(id=the_id) for the_id in ['income-bar', 'cost-bar', 'efficiency-bar', 'atlas-bubble']]

app = dash.Dash(__name__)
app.layout = html.Div([header, stock_section, industry_section, sub_industry_section] + charts)


# callback 定义
@app.callback(Output('industry-dropdown', 'value'), [Input('stock-dropdown', 'value')])
def update_industry_dropdown_value(stock_name):
    with DatabaseAPI() as database:
        industry, sub_industry = database.get_industry(stock_name=stock_name)
        return industry


@app.callback(Output('sub_industry-radio', 'options'), [Input('industry-dropdown', 'value')])
def update_sub_industry_dropdown_options(industry_name):
    with DatabaseAPI() as database:
        sub_industry_list = database.get_sub_industry_list(industry_name=industry_name)
        new_options = [{'label': each_sub, 'value': each_sub} for each_sub in sub_industry_list]
        return new_options


@app.callback(Output('sub_industry-radio', 'value'), [Input('stock-dropdown', 'value')])
def update_sub_industry_dropdown_value(stock_name):
    with DatabaseAPI() as database:
        industry, sub_industry = database.get_industry(stock_name=stock_name)
        return sub_industry


@app.callback(Output('income-bar', 'figure'), [Input('sub_industry-radio', 'value')])
def update_income_chart(sub_industry):
    with DatabaseAPI() as database:
        df = database.get_statements(sub_industry=sub_industry)
    return bar_figure(df=AnalysisAPI(df).income_df(), title='收入对比（亿）')


@app.callback(Output('cost-bar', 'figure'), [Input('sub_industry-radio', 'value')])
def update_cost_chart(sub_industry):
    with DatabaseAPI() as database:
        df = database.get_statements(sub_industry=sub_industry)
    return bar_figure(df=AnalysisAPI(df).cost_df(), title='成本拆解', stack=True, y_percent=True)


@app.callback(Output('efficiency-bar', 'figure'), [Input('sub_industry-radio', 'value')])
def update_efficiency_chart(sub_industry):
    with DatabaseAPI() as database:
        df = database.get_statements(sub_industry=sub_industry)
    return bar_figure(df=AnalysisAPI(df).efficiency_df(), title='运营效率对比', y_percent=True)


@app.callback(Output('atlas-bubble', 'figure'), [Input('sub_industry-radio', 'value')])
def update_atlas_chart(sub_industry):
    with DatabaseAPI() as database:
        industry, sub_industry = database.get_industry(sub_industry=sub_industry)
        df = database.get_statements(industry=industry, limit=None, with_price=True)

    title_text = '行业总览（{}）'.format(industry)

    return atlas_figure(df=AnalysisAPI(df).nice_companies(), title=title_text)


if __name__ == '__main__':
    app.run_server(debug=True)
