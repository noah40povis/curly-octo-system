import pandas as pd 
import numpy as np 

excel_file = 'Datasets.xlsx'


def auto(excel_file):
    assert isinstance(excel_file, str), 'Strings only!'
    #break up every sheet into a variable
    stdib1 = pd.read_excel(excel_file, 1)
    stdib2 = pd.read_excel(excel_file, 2)
    stdib3 = pd.read_excel(excel_file, 3)
    seller_address_0 = pd.read_excel(excel_file, 4)
    seller_address_1 = pd.read_excel(excel_file, 5)
    salesforce_account = pd.read_excel(excel_file, 6)
    salesforce_contact = pd.read_excel(excel_file, 7)
    final_format = pd.read_excel(excel_file, 8)
    #concat stdibs sheets 
    stdib_final = pd.concat([stdib2, stdib1, stdib3], axis=0)
    #merge 
    salesforce_final = salesforce_contact.merge(salesforce_account, on="seller_pk", how="outer", copy=False)
    #rename column id in order to merge on similar column name
    seller_address_1.rename(columns={"id":"address_id"}, inplace=True)
    #merge seller address sheets
    seller_address = seller_address_0.merge(seller_address_1, on='address_id', how= 'outer')
    #drop duplicates
    seller_address.drop_duplicates(subset=['seller_pk','address_id','address_type'], inplace=True)
    #fill null values for groupby function 
    seller_address.fillna('0', inplace=True)
    #groupby 
    seller_address_gb = seller_address.groupby(['seller_pk'])['address_type'].apply(','.join).reset_index()
    #convert columns data type to make sure they are unicode compliant 
    seller_address_gb.seller_pk = seller_address_gb.seller_pk.astype("unicode")
    seller_address_gb.address_type = seller_address_gb.address_type.astype("unicode")
    seller_address.state = seller_address.state.astype("unicode")
    seller_address.country = seller_address.country.astype("unicode")
    #get rid of duplicates to avoid merging issue 
    seller_address.drop_duplicates(subset=['seller_pk'], inplace=True)
    #reset index 
    seller_address.reset_index(inplace=True)
    #join dataframes but only include state and country 
    seller_final  = seller_address_gb.join(seller_address[['state','country']], lsuffix=['seller_pk'], rsuffix=['seller_pk'], how='inner')

    #create empty columns to populate with condition statements below 
    seller_final['Seller_Default_State'] = np.nan
    seller_final['Seller_Default_Country'] = np.nan
    seller_final['Seller_Shipping_State'] = np.nan
    seller_final['Seller_Shipping_Country'] = np.nan
    seller_final['Seller_Billing_State'] = np.nan
    seller_final['Seller_Billing_Country'] = np.nan

    #condition statements
    seller_final['Seller_Default_State'] = np.where(seller_final['address_type'].str.contains('SELLER_DEFAULT'), seller_final['state'], seller_final['Seller_Default_State'])

    seller_final['Seller_Default_Country'] = np.where(seller_final['address_type'].str.contains('SELLER_DEFAULT'), seller_final['country'], seller_final['Seller_Default_Country'])

    seller_final['Seller_Shipping_State'] = np.where(seller_final['address_type'].str.contains('SELLER_SHIPPING'), seller_final['state'], seller_final['Seller_Shipping_State'])

    seller_final['Seller_Shipping_Country'] = np.where(seller_final['address_type'].str.contains('SELLER_SHIPPING'), seller_final['country'], seller_final['Seller_Shipping_Country'])

    seller_final['Seller_Billing_State'] = np.where(seller_final['address_type'].str.contains('SELLER_BILLING'), seller_final['state'], seller_final['Seller_Billing_State'])

    seller_final['Seller_Billing_Country'] = np.where(seller_final['address_type'].str.contains('SELLER_BILLING'), seller_final['country'], seller_final['Seller_Billing_Country'])
    
    
    #final merges 
    # stdib_final.seller_pk.astype("unicode")
    join1 = stdib_final.merge(seller_final, on='seller_pk', how='outer')
    join2 = join1.merge(salesforce_final, on='seller_pk', how='left')

    
    #grab subset of dataframe
    sliced = join2[['seller_pk','seller_status','seller_status_code','seller_rating','seller_date_registered', 'seller_date_contract_start',                        'seller_distinguished','Seller_Default_State', 'Seller_Default_Country','Seller_Shipping_State',      'Seller_Shipping_Country','Seller_Billing_State', 'Seller_Billing_Country','contact_id','account_id']]
    
    
    #convert to est timezone 
    sliced['seller_date_contract_start'] = pd.to_datetime(sliced['seller_date_contract_start'], unit='s').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    sliced['seller_date_registered'] = pd.to_datetime(sliced['seller_date_registered'], unit='s').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

    
    #fill null values with empty space to fit format 
    sliced = sliced.replace('0','')

    #alter column to 1 v 0 to fit format 
    sliced['seller_distinguished'] = np.where(sliced['seller_distinguished']== 'DISTINGUISHED',1,0)
    

    #sort values by seller pk 
    sliced = sliced.sort_values('seller_pk')
    #rename columns 
    sliced = sliced.rename(columns={'seller_pk':'Seller PK', 'seller_status':'Seller Status', 'seller_status_code':'Seller Status                            (Full)', 'seller_rating':'Seller Rating',
                            'seller_date_registered':'Seller Registered Date', 'seller_date_contract_start':'Seller Agreement Start Date',
                            'seller_distinguished':'Seller Destinguished', 'Seller_Default_State':'Seller Default State',
                            'Seller_Default_Country':'Seller Default Country', 'Seller_Shipping_State':'Seller Shipping State',
                            'Seller_Shipping_Country':'Seller Shipping Country', 'Seller_Billing_State':'Seller Billing State',
                            'Seller_Billing_Country':'Seller Billing State', 'contact_id':'Seller Contact ID', 'account_id':'Seller Account ID'})

    sliced.drop([0], inplace=True)

    return sliced.to_csv('cleaned_sheet.csv', index=False)

auto(excel_file)



""" Considering the datatypes we are working with: text and numeric, 
    the amount of data: >1GB, performance need: instant querying is not critical 
    I would say it would be safe to use a relational database with a flat database model. 
"""


