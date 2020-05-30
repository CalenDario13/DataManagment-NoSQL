import numpy as np
import pandas as pd
import time
from statistics import mean, stdev

import matplotlib.pyplot as plt

import pymongo
from bson.code import Code
import utils as u
m = u.mongo()

PATH = '/Users/Dario/Google Drive/DS/First Year - Secon Semester/DM/homeworks/hw_3/data/'

# Create collections:
'''    
m.create_collections('olist', ['customers', 'orders', 'sellers'], PATH)
'''

# Crete connections:
    
cust_col = m.connect('olist', 'customers')
ords_col = m.connect('olist', 'orders')
sell_col = m.connect('olist', 'sellers')

# Query:
    
q11_nidx_t = []
for _ in range(50):
    s = time.time()
    q11 = ords_col.aggregate([
        {'$match': {'$and': [{'order_status': 'canceled'}, {'order_delivery_info.order_delivered_carrier_date':{'$ne': np.NaN}}]}},
        {'$unwind': '$order_items'},
        {'$group': {'_id': '$order_items.seller_id', 'orders_sent': {'$sum': 1}}},
        {'$lookup': {'from': 'sellers', 'localField': '_id', 'foreignField': 'seller_id', 'as': 'seller_order'}},
        {'$unwind': '$seller_order'},
        {'$project': {'_id': 0, 'seller_id': '$_id', 'state': '$seller_order.seller_address.seller_state', 'orders_sent': '$orders_sent'}},
        {'$sort': {'orders_sent': -1}}
        ])
    e = time.time()
    q11_nidx_t.append(e - s)
    q11_df = m.query(q11, prnt = False)
print('It takes aroun: {} s\n\n{}'.format(mean(q11_nidx_t), q11_df.head(30).to_string(index=False)))

# Create idxs for q4 (different syntax in mongo: db.collection.insertIndex({'attribute': 1/-1}, {'name': 'idx_name'})):
  
sell_col.create_index( [ ('seller_id', pymongo.ASCENDING)], name = "seller_id_idx", unique = True ) # on seller_id in sellers
ords_col.create_index( [ ('order_status', pymongo.ASCENDING) ], name =  "order_status_idx"  ) # on order_status in orders (not recommended)

q11_idx_t = []
for _ in range(50):
    s = time.time()
    q11 = ords_col.aggregate([
        {'$match': {'$and': [{'order_status': 'canceled'}, {'order_delivery_info.order_delivered_carrier_date':{'$ne': np.NaN}}]}},
        {'$unwind': '$order_items'},
        {'$group': {'_id': '$order_items.seller_id', 'orders_sent': {'$sum': 1}}},
        {'$lookup': {'from': 'sellers', 'localField': '_id', 'foreignField': 'seller_id', 'as': 'seller_order'}},
        {'$unwind': '$seller_order'},
        {'$project': {'_id': 0, 'seller_id': '$_id', 'state': '$seller_order.seller_address.seller_state', 'orders_sent': '$orders_sent'}},
        {'$sort': {'orders_sent': -1}}
        ])
    e = time.time()
    q11_idx_t.append(e - s)
    q11_df = m.query(q11, prnt = False)

print('It takes aroun: {} s\n\n{}'.format(mean(q11_idx_t), q11_df.head(30).to_string(index=False)))

# drop idx of q11:
sell_col.drop_index('seller_id_idx')
ords_col.drop_index('order_status_idx')

# Plot comparison result:
'''
plt.figure(figsize  = (14, 8))
plt.barh(y = ['idx', 'no idx'], width = [mean(q11_idx_t), mean(q11_nidx_t)], 
         color = ['darkorange', 'royalblue'], edgecolor = 'black', linewidth = 0.7,
         xerr= [1.96 * stdev(q11_idx_t),  1.96*stdev(q11_nidx_t)])
plt.xlabel('mean execution time (s)', size = 18)
plt.yticks(size = 16)
plt.xticks(size = 16)
plt.title('Query 4 execution with and without indexes', size = 20)
plt.show()
'''

# map reduce:
    
map = Code('function(){'
            'for (var idx=0; idx < this.order_items.length;idx++){'
                'var key = this.order_items[idx].product_category_name;'
                'var value = {"price":this.order_items[idx].price,"freight_value":this.order_items[idx].freight_value, "count":1};'
                'emit (key, value);};}')
reduce = Code('function (key, value){'
    'var reduced = {"price":0 , "freight_value":0, "count":0};'
    'for (var idx=0; idx<value.length;idx++){'
        'reduced["count"] += value[idx]["count"];'
        'reduced["price"] += value[idx]["price"];'
        'reduced["freight_value"] += value[idx]["freight_value"];};'
    'return (reduced);}')
final = Code('function (key , value){'
    'value["freight_value"] = value["freight_value"]/value["count"];'
    'return(value);}')

result = ords_col.map_reduce(map, reduce, 'myResult', finalize = final)
db_lst = []
for row in result.find():
    _id = row['_id']
    dic = row['value']
    dic['category'] = _id
    db_lst.append(dic)
    
q12_df = pd.DataFrame(db_lst)
q12_df = q12_df[ ['category', 'price', 'freight_value', 'count']]
print(q12_df.head(15).to_string(index=False))    

#################################

q12 = ords_col.aggregate([
    {"$unwind":"$order_items"},
    {"$group":{"_id":"$order_items.product_category_name", "tot_spent" : {"$sum":"$order_items.price"}, 
    "freight_value":{"$avg":"$order_items.freight_value"},"num_purchases":{"$sum":1}}},
    {"$sort":{"_id":1}}
    ])

q12_df = m.query(q12, prnt = True)

###################################

q1 = ords_col.aggregate([
    
    {'$group': {'_id': "$customer_unique_id", 'tot_purchases': {'$sum': 1}, 
                'order_payments': {'$push': '$order_payments'},
                'city':{'$first': '$order_delivery_info.shipping_address.customer_city'},
                'state':{'$first': '$order_delivery_info.shipping_address.customer_state'}}},
    {'$project': {"_id": 0, 'customer_unique_id': '$_id', 'city': '$city', 'state': '$state',
                  'tot_purchases': '$tot_purchases', 
                  'order_payments': {'$reduce':{'input': '$order_payments',
                                                'initialValue': [], 'in': {'$concatArrays':['$$value', '$$this']}}}}},
    {'$unwind': '$order_payments'},
    {'$group': {'_id': "$customer_unique_id", 'tot_spendings':{'$sum': '$order_payments.payment_value'}, 
                'tot_purchases':{'$first': '$tot_purchases'},
                'city':{'$first': '$city'},
                'state':{'$first': '$state'}}},
    {'$project': {'_id': 0, 'customer_unique_id': '$_id', 'state':'$state', 'city': '$city',
                  'tot_purchases': '$tot_purchases', 'tot_spendings': '$tot_spendings'}},
    {'$sort': {"tot_purchases": -1, "tot_spendings": -1}},
    {'$limit': 10}
    ])

q1_df = m.query(q1, prnt = True)

####################################

q3 = ords_col.aggregate([

    {"$unwind":"$order_items"},
    {'$match': {'order_items.product_category_name': {'$ne': np.NaN}}},
    {"$project":{"_id":0,"product_id":"$order_items.product_id", "category":"$order_items.product_category_name",
                 "reviews":{"$reduce":{
                            "input":"$order_reviews.review_score",
                            "initialValue":"None",
                            "in":{"$avg":["$$value", "$$this"]}
                 }}}},
    {"$group":{"_id":"$product_id", "num_purchased":{"$sum":1},"category":{"$first":"$category"}, "avg_review":{"$avg":"$reviews"}}},
    {"$project":{"_id":0, "product_id": "$_id","num_purchased":1,"category":1, "avg_review":1}},
    {"$group":{"_id" : "$category", "purchases":{"$push":"$num_purchased"},"max_purchase":{"$max":"$num_purchased"},
               "products":{"$push":"$product_id"},"avg_review":{"$push":"$avg_review"} }},
    {"$project":{"_id":0, "product_category_name":"$_id", 
                "product_id":{"$arrayElemAt":["$products",{"$indexOfArray":["$purchases", "$max_purchase"]}]}, 
                "tot_sales":"$max_purchase",
                "avg_score": {'$round': [{"$arrayElemAt":["$avg_review",{"$indexOfArray":["$purchases", "$max_purchase"]}]}, 2]}}},
    {"$sort":{"product_category_name":1}}
    ])

q3_df = m.query(q3, prnt = True)

################################

q4 = ords_col.aggregate([
    {'$project': {'order_items': '$order_items', 'order_payments': '$order_payments',
        'delivery_days': { '$dayOfMonth': {'$convert': {'input':
            {'$subtract': [
            {'$convert':
                {'input': {'$convert': {'input':'$order_delivery_info.order_delivered_customer_date', 'to':'long', 'onNull': 0, 'onError': 0}},
                'to':'date', 'onNull': None, 'onError': None}}, # None = ISODate() in Robot 3T
            {'$convert':
                {'input': {'$convert':{'input':'$order_delivery_info.order_delivered_carrier_date', 'to':'long', 'onNull': 0, 'onError': 0}},
                'to':'date', 'onNull': None, 'onError': None}}]},
            'to':'date', 'onNull': None, 'onError': None}}}}},
   {'$unwind': '$order_items'},
   {'$match': {'order_items.product_category_name': {'$ne': np.NaN}}}, # just np.NaN =  NaN in Robot 3T
    {'$group': {'_id': '$order_items.product_category_name', 'tot_sales': {'$sum': 1},
     'avg_price': {'$avg': '$order_items.price'}, 'avg_shipping_cost': {'$avg': '$order_items.freight_value'},
     'avg_delivered_carrier_date': {'$avg': '$delivery_days'},
     'order_payments':{'$push': '$order_payments'}}},
   {'$project': {'_id': 0, 'category': '$_id', 'tot_sales':'$tot_sales', 'avg_price':'$avg_price', 'avg_shipping_cost':'$avg_shipping_cost',
       'avg_delivered_carrier_date':'$avg_delivered_carrier_date', 
       'order_payments': {'$reduce': {'input': '$order_payments', 'initialValue': [], 'in': {'$concatArrays':['$$value', '$$this']}}}}},
   {'$unwind': '$order_payments'},
   {'$group': {'_id': '$category', 'tot_sales':{'$first': '$tot_sales'}, 'avg_price': {'$first':'$avg_price'}, 'avg_shipping_cost':{'$first':'$avg_shipping_cost'},
   'avg_installments': {'$avg': '$order_payments.payment_installments'}, 'avg_delivery_days': {'$first': '$avg_delivered_carrier_date'}}},
   {'$project': {'_id': 0, 'category': '$_id', 'tot_sales': '$tot_sales', 'avg_price': {'$round':['$avg_price', 0]}, 
    'avg_installments': {'$round': ['$avg_installments',1]}, 
    'avg_cost_of_installment': {'$round':[{'$divide': [{'$round':['$avg_price', 0]}, {'$round':['$avg_installments',1]}]},0]}, 
    'avg_shipping_cost': {'$round':['$avg_shipping_cost',0]}, 'avg_delivery_days': {'$round': ['$avg_delivery_days',0]}}},
   {'$sort': {'category': 1}}
])
    
q4_df = m.query(q4, prnt = True)

####################################

q9 = ords_col.aggregate([
    {"$unwind":"$order_items"},
    {"$group":{"_id":{"region":"$order_delivery_info.shipping_address.customer_state", "category":"$order_items.product_category_name"},
               "num_purchases":{"$sum":1}, "tot_spending":{"$sum":"$order_items.price"}}},
    {"$group":{"_id":"$_id.region", "categories":{"$push":"$_id.category"}, "purchases":{"$push":"$num_purchases"},
               "spendings":{"$push":"$tot_spending"}, "max_purchases":{"$max":"$num_purchases"}}},
    {"$project":{"_id":0, "geolocation_state":"$_id", 
                "product_category_name":{"$arrayElemAt":["$categories",{"$indexOfArray":["$purchases", "$max_purchases"]}]},
                "n_sales":"$max_purchases",
                "tot_spent":{"$arrayElemAt":["$spendings",{"$indexOfArray":["$purchases", "$max_purchases"]}]}}},
    {"$sort":{"geolocation_state":1}}    
    ])

q9_df = m.query(q9, prnt = True)

##################################

q2 = ords_col.aggregate([
    {'$unwind': '$order_items'},
    {'$group': {'_id': '$order_items.seller_id', 'tot_sales': {'$sum': 1},
                'tot_income': {'$sum': '$order_items.price'},
                'order_reviews': {'$push': '$order_reviews'}}},
    {'$project': {'_id': 0, 'seller_id': '$_id', 'tot_sales': '$tot_sales', 'tot_incomes': {'$round':  ['$tot_income', 0]},
                  'order_reviews': {'$reduce':{'input': '$order_reviews', 'initialValue': [], 'in': {'$concatArrays':['$$value','$$this']}}}}},           
    {'$unwind': '$order_reviews'},
    {'$group': {'_id': '$seller_id', 'tot_sales':{'$first': '$tot_sales'},
                'tot_incomes': {'$first': '$tot_incomes'}, 'avg_review': {'$avg': '$order_reviews.review_score'}}},
    {'$project': {'_id': 0, 'seller_id': '$_id', 'avg_review': {'$round': ['$avg_review', 2]},
                  'tot_sales': '$tot_sales', 'tot_incomes': '$tot_incomes',
                  'score': {'$round': [
                      {'$add': [{'$multiply': [{'$log':[{'$add':['$tot_sales',1]}, 10]}, 0.30]}, 
                                {'$multiply': [{'$log':['$tot_incomes', 10]}, 0.10]}, 
                                {'$multiply':['$avg_review',0.6]}]}, 
                                2]}
                  }},            
    {'$sort': { 'score': -1 }},
    {'$limit':10},
    
])

    
q2_df = m.query(q2, prnt = True)



    
    
    
    
    

