import streamlit as st
from PIL import Image

# -------------------------------
# PAGE CONFIG
# -------------------------------
st.set_page_config(
    page_title="Jumia Sales Databricks LakeFlow Pipeline",
    page_icon="📊",
    layout="wide"
)

# -------------------------------
# TITLE SECTION
# -------------------------------
st.title("📊 Jumia Sales — End-to-End Databricks LakeFlow Declarative Pipeline")
st.markdown("""
This project demonstrates how to build an **end-to-end data pipeline** on Databricks using
**LakeFlow Declarative Pipeline** with **Bronze → Silver → Gold → Business layers**.

**Case Study:** Jumia Sales (Nigeria + Ghana).  
**Technologies:** Databricks LakeFlow, Delta Live Tables (DLT), Streaming, Unity Catalog.

👉 [GitHub Repo](https://github.com/uka-ben/sales-pipeline--databricks-declarative-pipeline)  
👉 [YouTube Video](https://youtu.be/ywivtZhyjx4?si=p8XtlwK2EC7mdtZq)
""")

st.divider()

# -------------------------------
# PIPELINE WALKTHROUGH
# -------------------------------
st.header("🔄 Pipeline Walkthrough")

captions = {
    1: "Bronze Layer — Raw sales ingestion from Nigeria & Ghana.",
    2: "Bronze Layer — Customers ingestion with data quality rules.",
    3: "Silver Layer — Cleaned and joined sales with product & customer data.",
    4: "Gold Layer — Aggregated sales by region & category.",
    5: "Business Views — Materialized sales dashboard-ready tables.",
    6: "Streaming Updates — Handling Change Data Capture (CDC).",
    7: "Final Dashboard — Jumia Sales insights for decision-making."
}

for i in range(1, 8):  # screenshots 1 to 7
    col1, col2 = st.columns([1, 2])
    with col1:
        img = Image.open(f"screenshots/screenshot{i}.png")
        st.image(img, caption=f"Screenshot{i}", use_container_width=True)
    with col2:
        st.subheader(f"Step {i}")
        st.write(captions[i])
    st.divider()

# -------------------------------
# KEY LEARNINGS / IMPACT
# -------------------------------
st.header("📌 Key Learnings & Professional Impact")
st.markdown("""
- ✅ Designed a **Bronze → Silver → Gold Lakehouse architecture**.  
- ✅ Built **streaming ingestion pipelines** with Delta Live Tables (DLT).  
- ✅ Applied **data quality rules** and **Change Data Capture (CDC)**.  
- ✅ Created **Business Views** for analytics & dashboards.  
- ✅ Automated end-to-end pipeline orchestration in Databricks.  

**Recruiters should note:**  
I can design and implement **production-grade data pipelines** that handle both **batch + streaming ingestion** using Databricks LakeFlow, Unity Catalog, and DLT.
""")

st.success("🎯 This project is a template for enterprise-scale Lakehouse implementations.")

st.divider()

# -------------------------------
# ABOUT ME SECTION
# -------------------------------
st.header("👨‍💻 About Me")
st.markdown("""
**Benjamin Imo Uka**  
**AI & Data Engineering | ML Systems, LLMs, DRL | Databricks, PySpark, MLOps**

- Portfolio: [https://benjaminuka.streamlit.app/](https://benjaminuka.streamlit.app/)  
- GitHub: [https://github.com/uka-ben](https://github.com/uka-ben)  
- LinkedIn: [https://www.linkedin.com/in/benjamin-uka-imo](https://www.linkedin.com/in/benjamin-uka-imo)  
- YouTube: [https://youtube.com/@blackdatascience](https://youtube.com/@blackdatascience)  

I specialize in **end-to-end AI & Data Engineering solutions**:
- Databricks LakeFlow, AutoLoader, Streaming, DLT  
- ML Systems (training, deployment, MLOps)  
- Deep Reinforcement Learning (DRL), LLMs, GNNs, Predictive Systems  
- Projects in Finance, NLP, Fraud Detection, IoT, Robotics  

📌 I use projects like this one to **teach, build, and deliver real-world enterprise-grade solutions**.
""")

