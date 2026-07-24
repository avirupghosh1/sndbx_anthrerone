# Startup Guide

To start make own "our_api_key" in the portal [http://api.qa6-agent-sandbox.sprinklr.com/portal](http://api.qa6-agent-sandbox.sprinklr.com/portal)

This "our_api_key" can be used as E2B_API_KEY, DAYTONA_API_KEY, MODAL_TOKEN_SECRET

## For E2B

```bash
export E2B_API_URL=http://api.qa6-agent-sandbox.sprinklr.com
export E2B_API_KEY=our_api_key                  %%replace here
```

or pass **[ api_url = “http://api.qa6-agent-sandbox.sprinklr.com” , api_key=our_api_key ]** to every method call

- As current url’s are not ca certified

  **“https://*.qa6-agent-sandbox.sprinklr.com”** won’t work so we need to use inject sandbox_url **“http://49983-{sid}.qa6-agent-sandbox.sprinklr.com”** after every create using a reconnect. the code patch is something like this,

```python
sandbox= e2b.Sandbox.create( template_id, metadata={..}, api_url="http://api.qa6-agent-sandbox.sprinklr.com",api_key="e2b_..")
// extra add start
sid= getattr( sandbox, "sandbox_id", "")
sandbox= e2b.Sandbox.connect( sid, sandbox_url=f"http://49983-{sid}.qa6-agent-sandbox.sprinklr.com", api_url="http://api.qa6-agent-sandbox.sprinklr.com",api_key="e2b_..")
// extra add end
```

also whenever connecting via websocket use

```python
ws_url = f"ws://{sandbox.get_host(port)}"
ws_connection = websockets.connect( ws_url, **connection_args)
```

If ca cert is done for the wildcard domain we wont need the above and just api_url overwrite would work.

## For Daytona

```bash
export DAYTONA_API_URL=http://api.qa6-agent-sandbox.sprinklr.com
export DAYTONA_API_KEY=our_api_key                  %%replace here
```

or pass api_url to Daytonaconfig

```python
daytona = Daytona(DaytonaConfig(api_url="http://api.qa6-agent-sandbox.sprinklr.com", api_key="e2b_..", target="local"))
```

## For Modal

```bash
export MODAL_SERVER_URL=http://api.qa6-agent-sandbox.sprinklr.com:50051
export MODAL_TOKEN_SECRET=our_api_key.                        %%replace here
```

## Api documentation and more details

[http://api.qa6-agent-sandbox.sprinklr.com/docs](http://api.qa6-agent-sandbox.sprinklr.com/docs)