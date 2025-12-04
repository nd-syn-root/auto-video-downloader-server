from pydrive2.auth import GoogleAuth

gauth = GoogleAuth()
gauth.LocalWebserverAuth()  # Will open browser login window
gauth.SaveCredentialsFile("token.json")
