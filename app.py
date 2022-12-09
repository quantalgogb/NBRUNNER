import streamlit as st
import yaml
import os
import authenticate,live_daily_cpr
import streamlit_authenticator as stauth


st.set_page_config(
        page_title="NBRUNNER",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )

st.title('NBRUNNER📈')



with open('./cred.yaml') as file:
    config = yaml.safe_load(file)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)
name, authentication_status, username = authenticator.login('Login', 'main')

    
if st.session_state["authentication_status"]:
    authenticator.logout('Logout', 'main')
    st.markdown("""
        <style>
        .big-font {
            font-size:30px !important;
        }
        .small-font {
            font-size:20px !important;
        }
        </style>
        """, unsafe_allow_html=True)
    

    pt=f'Welcome {st.session_state["name"]}'
    st.markdown(f'<p class="big-font">{pt} 😊 !</p>', unsafe_allow_html=True)
    
    global data1,isauth,dcpr,slt
    data1={}
    isauth=False
    slt=False
    dcpr=False
    
    st.subheader('NOTEBOOK REPOSITORY')
    col1,col2=st.columns(2)
    with col1:
        st.markdown('<p class="small-font">DAILY LIVE CPR [PREVDAY CLOSE > MAX CPR ]</p>',unsafe_allow_html=True)
    with col2:
        daily_cpr=st.button("RUN NOTEBOOK",key='1')
        if daily_cpr:
            data1=authenticate.gen_acc_token()
            isauth=True
            dcpr=True
            #st.write(data1)
    
    if isauth==True and dcpr==True:
        st.success(data1['detail'])
        st.success('DAILY LIVE CPR [PREVDAY CLOSE > MAX CPR ] NOTEBOOK EXECUTED !')
        st.write('NOTE : It takes few minutes to load historical data and calculate CPR, please have patience.')
        live_daily_cpr.start(data1['acc_key'],data1['api_key'])
            
    col1,col2=st.columns(2)
    with col1:
        st.markdown('<p class="small-font">STOPLOSS - TARGET LIVE</p>',unsafe_allow_html=True)
    with col2:
        sl_target=st.button("RUN NOTEBOOK",key='2')
        if sl_target:
            slt=True
                
    
    if slt:
        st.warning('COMING SOON !')
        
    
    
    
elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')