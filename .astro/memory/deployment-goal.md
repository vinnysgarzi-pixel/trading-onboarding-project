# Astro Deployment Goal

### Source
user-interaction - The user clarified the intended destination for the onboarding project.

## Memory
This onboarding project is intended to be deployed to Astro, not only run locally. Architecture choices should favor deployable Astro patterns: durable external storage, Airflow Variables/Connections for configuration and secrets, and dependencies in `requirements.txt`.

## Context
The initial MVP used local DuckDB storage for fast scaffolding, but deployment to Astro changes the preferred production/demo architecture.

## Evidence
- User said: "remember, the goal is to deploy this to Astro. Does that change anything about your architecture decisions?"
