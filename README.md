# DFW Arrival Radar

This is a small local website that tracks today's DFW arrivals, groups them by terminal and gate, and highlights flights expected to land ahead of schedule.

## Live demo

- Demo URL: [https://dfw-arrival-radar.onrender.com/](https://dfw-arrival-radar.onrender.com/)
- Note: this demo runs on Render's free tier, so the first load can take up to about a minute if the service has been idle.

## Run it

Use the bundled Python runtime:

```powershell
& "C:\Users\ZBOOK\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe" .\server.py
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

## Notes

- Data is scraped live from public DFW arrival pages on `airport-dallas.com`.
- The backend caches responses for 5 minutes to keep page loads fast and avoid hammering the source.
- If a detail page fails, the dashboard still shows the rest of the traffic board.

## Publish it

This project is now deployable on any host that can run a Docker container.

### Render

1. Push this folder to a GitHub repo.
2. In Render, create a new `Web Service`.
3. Point Render at the repo.
4. Choose the `Docker` runtime.
5. Render will detect `Dockerfile` and use `render.yaml` for the service settings.

The app now exposes a production health check at `/healthz` and binds to `0.0.0.0`, which public hosts require.

### Railway

1. Push this folder to GitHub, or use the Railway CLI from the repo root.
2. Create a new Railway service from the repo.
3. Railway will build from the included `Dockerfile`.

### What changed for deploys

- `Dockerfile`: containerizes the app.
- `.dockerignore`: keeps logs and local cache files out of the image.
- `render.yaml`: defines a Render web service with a health check.
- `server.py`: now supports `HOST` / `PORT` from the environment and exposes `/healthz`.
