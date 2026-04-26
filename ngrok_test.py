from pyngrok import ngrok
import time

ngrok.set_auth_token("3CnzQxXGceDI2CpS0Uu6NKfvoO2_tjZ3TZ4tmcRFRXxub32c")

url = ngrok.connect(8000)
print(url)

while True:
    time.sleep(1)