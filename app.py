from flask import Flask
from flask import Flask, request, jsonify, make_response, send_file, Response
app = Flask('app')
import numpy as np
import requests
from flask_cors import CORS, cross_origin
import datetime
import utils
import ast
import matplotlib
from matplotlib import pyplot as plt
matplotlib.pyplot.switch_backend('Agg') 
import sic_per_country
import sci_per_country
import inst, countries, world_bank
CORS(app, support_credentials=True)
app.config['CORS_HEADERS'] = 'Content-Type'



@app.route('/get_table_names', methods=['GET'])
def get_table_names():
    "get the table names"
    csv_name=utils.get_all_table_names()
    with open(csv_name) as fp:
        csv = fp.read()
    return Response(
        csv,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename="+csv_name})


@app.route('/plot_cumulative_scores_per_country', methods=['POST'])
def plot_cumulative_scores_per_country():
    "plots the cumulative scores per country"
    
    # get email
    type_of_plot=""
    type_of_plot=request.form.get('type_of_plot', type_of_plot)
    print("type_of_plot: ", type_of_plot)

    countries=[]
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    print(x)
    countries=x['countries']
    

    img_name=utils.plot_cumulative_scores_per_country(countries)
    return send_file(img_name, mimetype='image/jpg')
    

@app.route('/plot_h2020_sci_scores_per_country', methods=['POST'])
def plot_h2020_sci_scores_per_country():
    "plots the h2020_sci_scores_per_country"
    
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    countries=x['countries']
    
    img_name=utils.plot_h2020_sci_scores_per_country(countries)
    return send_file(img_name, mimetype='image/jpg')

@app.route('/get_sic_major_minor_score_per_country', methods=['POST'])
def get_sic_major_minor_score_per_country():
    "returns the sic stats per country"
    
    data,global_minor_avg,global_major_avg,minor_std,major_std=sic_per_country.get_average_major_and_minor_score_per_country()
    resp={}
    resp['data']=data
    resp['global_minor_avg']=global_minor_avg
    resp['global_major_avg']=global_major_avg
    resp['minor_std_dev']=minor_std
    resp['major_std']=major_std
    
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/get_sci_major_minor_score_per_country', methods=['POST'])
def get_sci_major_minor_score_per_country():
    "returns the sci stats per country"
    
    data,global_minor_avg,global_major_avg,minor_std,major_std=sci_per_country.get_average_major_and_minor_score_per_country()
    resp={}
    resp['data']=data
    resp['global_minor_avg']=global_minor_avg
    resp['global_major_avg']=global_major_avg
    resp['minor_std_dev']=minor_std
    resp['major_std']=major_std
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/get_sci_major_minor_score_per_inst', methods=['POST'])
def get_sci_major_minor_score_per_inst():
    "returns the sci stats per inst"
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    insts=x['insts']
    print("insts: ", insts)
    data,global_minor_avg,global_major_avg=sci_per_country.get_average_major_and_minor_sci_per_inst(insts)
    resp={}
    resp['data']=data
    resp['global_minor_avg']=global_minor_avg
    resp['global_major_avg']=global_major_avg
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/compare_inst', methods=['POST'])
def compare_inst():
    "gives comparison between various insts"
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    insts=x['insts']
    print("insts: ", insts)
    
    fields=x['fields']
    print("fields: ", fields)
    
    data=inst.compare_insts(insts,fields)
    resp=data
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/compare_countries', methods=['POST'])
def compare_countries():
    "gives comparison between various countries"
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    con=x['countries']
    print("con: ", con)
    
    fields=x['fields']
    print("fields: ", fields)
    
    data=countries.compare_countries(con,fields)
    resp=data
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/get_inst_info', methods=['POST'])
def get_inst_info():
    "get inst info"
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    insts=x['inst'][0]
    print("insts: ", insts)
    
    
    data=inst.get_inst_info(insts)
    resp=data
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/get_country_info', methods=['POST'])
def get_country_info():
    "get country info"
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    con=x['country'][0]
    print("country: ", con)
    
    
    data=countries.get_countries_info(con)
    resp=data
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


@app.route('/get_world_bank_country_profile', methods=['POST'])
def get_world_bank_country_profile():
    "get country profile by world bank analysis"
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    con=x['country'][0]
    print("country: ", con)
    
    
    data=world_bank.get_country_profile(con)
    resp=data
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp

@app.route('/get_world_bank_best_countries', methods=['POST'])
def get_world_bank_best_countries():
    "get country profile by world bank analysis"
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    field=x['field'][0]
    print("field: ", field)
    
    
    data=world_bank.get_best_countries(field)
    resp=data
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp

@app.route('/get_world_bank_countries_trend', methods=['POST'])
def get_world_bank_countries_trend():
    "get country trend by world bank analysis"
    
    x=request.get_data(parse_form_data=True)
    x=ast.literal_eval(x.decode("utf-8"))
    con=x['country'][0]
    print("country: ", con)
    
    
    data=world_bank.get_trend_of_data(con)
    resp=data
    resp=jsonify(resp)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=3000)