# Use case explanation – what each industry pack is about

Every pack runs the same framework and the same notebooks. What changes is the business story. This page explains that story for each industry, so you can pick the right pack for your audience and know what you're building before you start.

## What's the same in every pack

Every pack follows one pattern: a fictional organisation looks after a set of **things** (assets, accounts, stores, wards…). **Events** happen to those things (work orders, cases, admissions…). **Readings** arrive every 15 minutes and tell you how each thing is behaving, and a **CRM** holds the people involved. The Gold layer brings it all together into a **health scorecard**: every thing gets a 0–100 risk score, a High / Medium / Low band and an illustrative dollar figure, so the business can see what needs attention first and what it's worth.

The same demo moments happen in every pack:

- **Day 1:** messy data – technical and business data-quality rules, warnings vs quarantine.
- **Day 2:** events arrive for a thing the source hasn't sent yet. They're quarantined, not lost.
- **Day 3:** the missing thing arrives and its events resolve automatically. A new column appears in the source (schema drift). Two CRM records disappear (soft delete). One thing breaches its threshold from 12:00 to 18:00 and its risk score jumps.
- **Live onboarding:** an extra public or business feed is added with metadata rows only – no new pipeline or notebook.
- **Real time:** an alert stream with a burst of alerts for the Real-Time Intelligence demo.

The dollar figures are made up for the demo. Always present them as illustrative estimates.

## Quick pick

| Pack            | Fictional organisation       | The use case in one line                                                         | Dollar figure on the scorecard |
| --------------- | ---------------------------- | -------------------------------------------------------------------------------- | ------------------------------ |
| `energy`        | Southern Grid Transmission   | Which network assets are most likely to fail, and what's an outage worth?        | Outage cost at risk            |
| `telco`         | Southern Cross Telecom       | Which parts of the network are congested or faulty, and which customers feel it? | Revenue at risk                |
| `fsi`           | Harbour Mutual Bank          | One view of fraud and credit risk per account                                    | Expected loss                  |
| `manufacturing` | Kestrel Components           | Predictive maintenance – catch machines before they break down                   | Downtime cost at risk          |
| `healthcare`    | Coastal Health Network       | Patient flow – which wards are full and where the pressure is                    | Access-block cost at risk      |
| `ites`          | Bluegum Digital Services     | Client SLAs and contract health for an IT services provider                      | Contract revenue at risk       |
| `government`    | Riverbend Regional Authority | Public infrastructure and citizen cases against service standards                | Cost at risk                   |
| `retail`        | Wattle & Co                  | Stock availability – where we're losing sales to empty shelves                   | Lost sales at risk             |
| `digital`       | Lyrebird Apps                | Platform reliability and customer health for a SaaS business                     | ARR at risk                    |

---

## Energy & utilities – asset health and maintenance

**Organisation:** Southern Grid Transmission, an electricity transmission network operator in regional NSW.
**Audience:** network operations and asset management at electricity transmission and distribution network operators.

**The business problem:** the network has thousands of ageing assets. Maintenance teams need to know which ones are at risk *now*, combining what the asset register says, what the work order history shows and what the sensors are reading.

**What you build:** 240 assets (substations, transformers, breakers, capacitor banks, lines) with criticality and history, from the asset management system. Work orders (preventive, corrective, inspection, emergency) with cost and labour hours. Transformer telemetry every 15 minutes (oil temperature, load, ambient temperature) from SCADA. Landholders from Salesforce, with the property lot reference hashed.

**The moments that land:** a transformer's oil temperature goes over 95 °C in the afternoon and it jumps up the risk list. BoM weather (fire danger and wind gusts) is onboarded live and compared with emergency work orders by region.

**Dollar figure:** outage cost at risk = $6,000 per open work order + $2,500 per 15-minute window over the oil-temperature limit + rating (MVA) × criticality × $20.

## Telecommunications – network service assurance

**Organisation:** Southern Cross Telecom, a mobile, fixed and NBN operator.
**Audience:** network operations and service assurance at mobile, fixed and wholesale network operators.

**The business problem:** the network operations centre needs to see which cells and network elements are congested or faulty before customers start calling.

**What you build:** 240 network elements (4G/5G cells, core routers, OLTs, microwave links) from the network inventory system. Fault tickets (outage, degradation, hardware, configuration). Cell KPIs every 15 minutes (PRB utilisation, active users, call drop rate) from the OSS. Customers from Dynamics 365, with the ID document number dropped.

**The moments that land:** a cell goes over 90% PRB utilisation in the afternoon. A major events calendar (concerts, sport) is onboarded live and compared with faults by region – the "stadium surge" story.

**Dollar figure:** revenue at risk = $4,000 per open fault + $600 per congested 15-minute window + capacity (users) × criticality × $0.50.

## Financial services – fraud, credit risk and collections

**Organisation:** Harbour Mutual Bank, a retail and business bank.
**Audience:** fraud, credit risk and collections at retail and business banks, mutuals and credit unions.

**The business problem:** fraud, collections and payments teams each work from their own system, so nobody has one view of risk on an account. The bank wants a single ranked list of accounts that need attention, with an expected loss on each.

**What you build:** 240 accounts (transaction, savings, personal loan, home loan, credit card) with a credit risk grade, balance and status (performing, dormant, in arrears, closed), with history from core banking. Risk cases – card fraud, scam payment, account takeover and hardship / arrears – with amount and analyst hours. Transaction monitoring per account in 15-minute windows (declined rate, value, count). Customers from Salesforce Financial Services Cloud, with the tax file number dropped and email and mobile masked.

**The moments that land:** an account's declined transaction rate goes over 30% in the afternoon – a card-testing pattern. Core banking adds a `hardship_flag` column (schema drift). The public Scamwatch feed is onboarded live and compared with the bank's own cases by state.

**Dollar figure:** expected loss = $3,500 per open case + $250 per suspicious 15-minute window + balance × risk grade × 0.2%.

## Manufacturing – predictive maintenance

**Organisation:** Kestrel Components, an automotive and agricultural components manufacturer.
**Audience:** plant operations and maintenance at discrete and process manufacturers, including food and beverage.

**The business problem:** unplanned breakdowns on bottleneck machines stop the line. The plant wants to spot machines that are heading for failure and plan maintenance around them.

**What you build:** 240 machines (CNC mills, injection moulders, stamping presses, robot cells, conveyors) across plants, from the MES. Maintenance orders (breakdown, preventive, calibration, changeover) with parts cost and downtime hours. Machine telemetry every 15 minutes (vibration, units produced, motor temperature) from the IoT historian. Distributor contacts from Dynamics 365 Sales, with the driver licence number dropped.

**The moments that land:** a machine's vibration goes over 7.1 mm/s in the afternoon. Wholesale energy prices are onboarded live and compared with breakdowns by plant.

**Dollar figure:** downtime cost at risk = $5,000 per open maintenance order + $3,000 per 15-minute window over the vibration limit + rated units per hour × criticality × $2.

## Healthcare – patient flow and bed capacity

**Organisation:** Coastal Health Network, a public health service running five hospitals.
**Audience:** patient flow and clinical operations (state health departments, local health districts, private hospital groups, aged care).

**The business problem:** emergency departments back up when wards are full (access block). Bed managers need to see where occupancy is high, which admissions are waiting and where the pressure is coming from.

**What you build:** 120 wards (ED, ICU, medical, surgical, maternity) with beds and acuity level, from the patient administration system. Admissions (emergency, elective, transfer, maternity) with episode cost and nursing hours. Ward occupancy every 15 minutes (bed occupancy, patients waiting for a bed, nurses on shift). Patients from Dynamics 365, with the Medicare number hashed.

**The moments that land:** a ward goes over 95% occupancy in the afternoon. Influenza surveillance is onboarded live and compared with emergency admissions by hospital.

**Dollar figure:** access-block cost at risk = $1,500 per open admission + $2,000 per 15-minute window over 95% occupancy + beds × acuity × $500.

## IT & ITeS – client SLAs and contract health

**Organisation:** Bluegum Digital Services, an IT services provider running managed services for enterprise clients.
**Audience:** service delivery, SLA and account management at IT services providers, managed service providers and internal IT teams.

**The business problem:** an IT services firm earns its money on contracts with SLAs. Delivery leads need to see which client services are drifting towards an SLA breach, and how much contract revenue that puts at risk.

**What you build:** 240 client services (managed infrastructure, application support, cloud operations, service desk, security operations), each with an SLA tier, monthly fee and status (live, at risk, in transition, exited), from the ITSM system. Incidents (outage, performance, security, service request) with SLA penalties and effort hours. Service performance every 15 minutes (p95 response time, availability, ticket backlog) by delivery centre. Client contacts from HubSpot, with the secondary mobile dropped.

**The moments that land:** a client service's response time goes over 2,500 ms in the afternoon. The change calendar is onboarded live and compared with incidents by delivery centre – "do our changes cause our incidents?"

**Dollar figure:** contract revenue at risk = $2,000 per open incident + $500 per slow 15-minute window + monthly fee × SLA tier × 1%.

## Government – public infrastructure and citizen services

**Organisation:** Riverbend Regional Authority, a government authority delivering local infrastructure and citizen services. It's deliberately worded so it works for a local council or a state agency.
**Audience:** public sector operations and citizen services (state agencies, councils, transport, water and emergency services).

**The business problem:** the authority has to keep roads, bridges, drains and public buildings safe, and respond to citizen cases within published service standards – especially during storms and floods.

**What you build:** 240 public assets (road segments, bridges, public buildings, stormwater pits, streetlights) with criticality and replacement value, from the asset and works system. Citizen cases (road repair, flooding and drainage, public safety hazard, permit application) with cost, crew hours and service standards. Smart infrastructure sensors every 15 minutes (water level, vehicle count, PM2.5). Citizens from the Salesforce Public Sector portal, with the concession card number hashed.

**The moments that land:** a stormwater pit's water level goes over 150 cm in the afternoon. BoM rainfall and river heights are onboarded live and compared with flooding cases by district.

**Dollar figure:** cost at risk = $2,500 per open case + $1,200 per 15-minute window over the water-level limit + replacement value ($k) × criticality × $0.50.

## Retail – stock availability and lost sales

**Organisation:** Wattle & Co, a national supermarket and general merchandise retailer.
**Audience:** store operations, availability and supply chain at supermarkets, general merchandise and specialty retailers.

**The business problem:** empty shelves are lost sales, and they spike around promotions. The retailer wants to see which stores are losing the most to stock issues, and fix them faster.

**What you build:** 240 stores (supermarket, express, megastore, dark store, pharmacy) with tier and average weekly sales, from the back office and inventory system. Stock issues (out of stock, short delivery, cold-chain breach, price or label error) with lost sales and units affected. Store trading every 15 minutes (% of lines out of stock, sales, footfall) from POS and stock-on-hand. Loyalty members from Salesforce Marketing Cloud, with the payment card token hashed.

**The moments that land:** a store goes over 8% of lines out of stock in the afternoon. The promotions calendar is onboarded live and compared with out-of-stocks by state – "are half-price weeks emptying our shelves?" A freezer fault still features as one of the real-time alerts.

**Dollar figure:** lost sales at risk = $1,800 per open stock issue + $900 per 15-minute window over 8% out of stock + weekly sales ($k) × store tier × $1.

## Digital natives – platform reliability and customer health

**Organisation:** Lyrebird Apps, a B2B SaaS scale-up selling a collaboration platform.
**Audience:** product, growth and customer success at SaaS scale-ups and fast-growing start-ups.

**The business problem:** in SaaS, a slow or broken product turns into churn. The company wants a tenant health view that combines product performance, support load and plan size, so customer success knows who to call first.

**What you build:** 240 tenants (free to enterprise plans) with seats, account tier and status (active, trial, past due, churned), from the product database. Support tickets (bug, outage, billing, how-to) with credits issued and agent hours. Product usage every 15 minutes (p95 API latency, active users, error rate). App users from HubSpot, with the last sign-in IP address hashed.

**The moments that land:** a tenant's p95 latency goes over 800 ms in the afternoon. Marketing campaigns are onboarded live and compared with support load by cloud region – "did that campaign bring in users our support team couldn't handle?"

**Dollar figure:** ARR at risk = $1,500 per open ticket + $400 per slow 15-minute window + seats × account tier × $12.

---

Want a pack shaped around a specific customer? It's one JSON file – see [docs/ADD_INDUSTRY.md](docs/ADD_INDUSTRY.md).
