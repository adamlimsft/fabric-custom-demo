# Contributing

If you'd like a hand setting this up for a customer session or a training, feel free to email/message me (Adam Li liadam@microsoft.com). I'm happy to walk you through it or do a dry run with you.

---

Thanks for using the kit. A few ground rules so it stays easy for everyone to pick up.

## Found a problem?

Open an issue and tell me:

- which pack you were running (`energy`, `retail`, …) and which area (notebooks, pipelines, MLV, Warehouse, RTI, Dataflow Gen2, Power BI or AI);
- the notebook or file, and the cell or step where it failed;
- the error message, and a screenshot if the Fabric UI looked different from the guide.

The Fabric UI and previews (MLV, ontology, data agent) change often, so "the button has moved" issues are useful too.

## Want to change something?

Open an issue first so we can agree the approach, then send a pull request. Keep these in mind:

- **Keep every pack the same shape.** The notebooks, pipelines and guides work for all nine industries because each pack has the same objects (entity, event, reading, party, extra feed). Change the framework, not one pack.
- **Edit the config, then rebuild.** Most files are generated from `config/<pack>.json`. Change the config or the generator in `tools/`, then run `python tools/build_kit.py` and `python tools/local_check.py` (standard library only). Don't hand-edit generated SQL, KQL, M or the embedded notebook config – the next rebuild will overwrite it.
- **Adding an industry?** Follow [docs/ADD_INDUSTRY.md](docs/ADD_INDUSTRY.md).
- **Fictional data only.** No real customer names, people, identifiers or customer data anywhere in the repo – not in configs, prompts, screenshots or examples.
- **Keep the READMEs practical.** Short steps, what to click, what to say while it runs.
