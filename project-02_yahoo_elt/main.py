import requests

url = "https://yahoo-finance166.p.rapidapi.com/api/news/list-by-symbol"

querystring = {"s":"ACWI, VOO, CSPX","snippetCount":"500"}

headers = {
	"x-rapidapi-key": "b9c3b527b9msh9817fe4d53ac0eap166cf5jsnb45e992f316c",
	"x-rapidapi-host": "yahoo-finance166.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())