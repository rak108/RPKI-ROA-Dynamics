# IPXO ROA Dynamics Analysis
This project analyzes **Resource Public Key Infrastructure (RPKI) Route Origin Authorizations (ROAs)** for prefixes leased via **IPXO**, focusing on the Magellan IPXO platform. ROAs define which Autonomous Systems (ASNs) are authorized to originate a prefix. IPXO frequently issues, updates, or revokes these ROAs to manage leased address space. Our work is trying to analyze and understand these dynamics.  

## Objectives
- Measure prefix-level ROA **events** across daily snapshots:
  - **Creations:** Prefixes newly authorized to AS834 or present in Magellan IPXO repository.  
  - **Deletions:** Prefixes no longer authorized.  
  - **Updates:** Prefixes switching authorization **to** or **from** AS834/Magellan IPXO repository.  
- Analyze the **frequency, patterns, and lifetimes** of ROAs for leased prefixes.

## Structure

Pipeline of files (all in scripts dir) so far -
![Data Pipeline](./img/curr.png)
