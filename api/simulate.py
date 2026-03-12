import io
import json
import random
import uuid
from typing import AsyncGenerator

from db import get_conn
from ingest import embed_text, get_minio, ensure_bucket

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
    "Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
    "Wei", "Priya", "Raj", "Mei", "Hiroshi", "Yuki", "Carlos", "Maria",
    "Ahmed", "Fatima", "Olga", "Ivan", "Sanjay", "Aisha", "Diego", "Sofia",
    "Kenji", "Sakura", "Arjun", "Deepika", "Omar", "Layla", "Nikolai", "Elena",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
    "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell",
    "Mitchell", "Carter", "Roberts", "Chen", "Wang", "Li", "Zhang", "Liu",
    "Patel", "Kumar", "Singh", "Shah", "Kim", "Park", "Choi", "Tanaka",
    "Yamamoto", "Sato", "Nakamura", "Muller", "Schmidt", "Fischer", "Weber", "Meyer",
    "Ivanov", "Petrov", "Kowalski", "Novak", "Johansson", "Larsson", "Andersen", "Berg",
]

SKILL_POOLS = {
    "backend": ["Python", "Java", "Go", "Rust", "C++", "Node.js", "Ruby", "PHP", "Scala", "Kotlin",
                 "Django", "Flask", "FastAPI", "Spring Boot", "Express.js", "Rails", "gRPC", "GraphQL"],
    "frontend": ["React", "Vue.js", "Angular", "TypeScript", "JavaScript", "HTML", "CSS", "Svelte",
                  "Next.js", "Nuxt.js", "Tailwind CSS", "Redux", "Webpack", "Vite"],
    "data": ["SQL", "PostgreSQL", "MySQL", "MongoDB", "Redis", "Elasticsearch", "Kafka", "Spark",
             "Hadoop", "Airflow", "dbt", "Snowflake", "BigQuery", "Redshift"],
    "devops": ["Docker", "Kubernetes", "AWS", "GCP", "Azure", "Terraform", "Ansible", "Jenkins",
               "GitHub Actions", "GitLab CI", "Prometheus", "Grafana", "Linux", "Nginx"],
    "ml": ["TensorFlow", "PyTorch", "scikit-learn", "Pandas", "NumPy", "Hugging Face", "MLflow",
            "LangChain", "OpenCV", "NLP", "Computer Vision", "LLMs", "RAG", "BERT", "GPT"],
    "mobile": ["Swift", "Kotlin", "React Native", "Flutter", "iOS", "Android", "Xcode", "Jetpack Compose"],
    "security": ["OWASP", "Penetration Testing", "SOC 2", "SIEM", "Splunk", "Nessus", "Burp Suite", "IAM"],
    "product": ["Agile", "Scrum", "Jira", "Confluence", "Figma", "A/B Testing", "Product Strategy"],
}

TITLE_POOLS = {
    "backend": ["Software Engineer", "Backend Developer", "Senior Software Engineer", "Staff Engineer",
                 "Principal Engineer", "Engineering Lead", "API Developer"],
    "frontend": ["Frontend Developer", "UI Engineer", "Senior Frontend Engineer", "Full Stack Developer",
                  "Web Developer", "UX Engineer"],
    "data": ["Data Engineer", "Database Administrator", "Senior Data Engineer", "Analytics Engineer",
             "Data Architect", "ETL Developer"],
    "devops": ["DevOps Engineer", "Site Reliability Engineer", "Platform Engineer", "Cloud Engineer",
               "Infrastructure Engineer", "Release Engineer"],
    "ml": ["Machine Learning Engineer", "Data Scientist", "AI Engineer", "Research Scientist",
            "NLP Engineer", "ML Ops Engineer", "AI Researcher"],
    "mobile": ["iOS Developer", "Android Developer", "Mobile Engineer", "Senior Mobile Developer"],
    "security": ["Security Engineer", "Cybersecurity Analyst", "AppSec Engineer", "Security Architect"],
    "product": ["Product Manager", "Technical Product Manager", "Senior Product Manager", "Product Owner"],
}

EDUCATION = [
    {"degree": "B.S. Computer Science", "school": "MIT"},
    {"degree": "B.S. Computer Science", "school": "Stanford University"},
    {"degree": "B.S. Software Engineering", "school": "UC Berkeley"},
    {"degree": "M.S. Computer Science", "school": "Carnegie Mellon University"},
    {"degree": "M.S. Data Science", "school": "Columbia University"},
    {"degree": "M.S. Artificial Intelligence", "school": "Georgia Tech"},
    {"degree": "B.S. Information Technology", "school": "University of Michigan"},
    {"degree": "B.S. Electrical Engineering", "school": "Purdue University"},
    {"degree": "Ph.D. Computer Science", "school": "University of Washington"},
    {"degree": "B.S. Mathematics", "school": "UCLA"},
    {"degree": "M.S. Machine Learning", "school": "ETH Zurich"},
    {"degree": "B.S. Computer Science", "school": "University of Texas at Austin"},
    {"degree": "M.B.A.", "school": "Harvard Business School"},
    {"degree": "B.S. Computer Science", "school": "University of Illinois"},
    {"degree": "M.S. Computer Science", "school": "University of Toronto"},
    {"degree": "B.S. Computer Science", "school": "National University of Singapore"},
    {"degree": "M.S. Computer Science", "school": "IIT Bombay"},
    {"degree": "B.S. Computer Science", "school": "Tsinghua University"},
    {"degree": "B.A. Computer Science", "school": "University of Oxford"},
    {"degree": "B.S. Computer Science", "school": "State University"},
    {"degree": "Associate Degree in IT", "school": "Community College"},
    {"degree": "Bootcamp Certificate", "school": "General Assembly"},
    {"degree": "Bootcamp Certificate", "school": "Hack Reactor"},
    {"degree": "B.S. Physics", "school": "Caltech"},
]

CLEARANCE_LEVELS = [
    "Top Secret/SCI", "Top Secret", "Secret", "Confidential", "Public Trust",
]

# Companies where clearance is more likely
CLEARED_COMPANIES = [
    "Lockheed Martin", "Raytheon", "Northrop Grumman", "General Dynamics",
    "BAE Systems", "L3Harris Technologies", "Leidos", "SAIC", "Booz Allen Hamilton",
    "ManTech", "CACI International", "Peraton", "Parsons Corporation",
]

COMPANIES = [
    "Google", "Amazon", "Meta", "Apple", "Microsoft", "Netflix", "Uber", "Airbnb",
    "Stripe", "Shopify", "Salesforce", "Oracle", "IBM", "Intel", "Cisco", "VMware",
    "Datadog", "Snowflake", "Palantir", "Databricks", "Confluent", "HashiCorp",
    "Twilio", "Cloudflare", "CrowdStrike", "Okta", "MongoDB Inc.", "Elastic",
    "Acme Corp", "TechStart Inc.", "DataFlow Systems", "CloudNine Solutions",
    "NexGen Software", "Quantum Analytics", "ByteWorks", "CodeCraft Studios",
    "Innovate Labs", "Digital Dynamics", "Vertex Technologies", "Apex Engineering",
    "Frontier AI", "Nimbus Cloud", "Spark Digital", "Atlas Computing",
    "JPMorgan Chase", "Goldman Sachs", "Morgan Stanley", "Capital One",
    "Deloitte", "McKinsey", "Accenture", "PwC", "KPMG", "EY",
] + CLEARED_COMPANIES

ACHIEVEMENTS = [
    "Reduced API latency by {pct}% through query optimization",
    "Led migration of {n} microservices to Kubernetes",
    "Built real-time data pipeline processing {n}M events/day",
    "Increased test coverage from {low}% to {high}%",
    "Mentored team of {n} junior developers",
    "Reduced infrastructure costs by ${k}K/year",
    "Implemented CI/CD pipeline reducing deploy time by {pct}%",
    "Designed and launched feature used by {n}K+ users",
    "Improved model accuracy from {low}% to {high}%",
    "Architected system handling {n}K requests/second",
    "Led cross-functional team of {n} engineers",
    "Reduced incident response time by {pct}%",
    "Built automated testing framework saving {n} hours/week",
    "Migrated legacy monolith to microservices architecture",
    "Implemented zero-downtime deployment strategy",
]


def _rand_achievement() -> str:
    tmpl = random.choice(ACHIEVEMENTS)
    return tmpl.format(
        pct=random.randint(20, 80),
        n=random.randint(3, 50),
        low=random.randint(40, 60),
        high=random.randint(80, 99),
        k=random.randint(50, 500),
    )


def generate_candidate() -> dict:
    """Generate a single mock candidate with structured data and resume text."""
    # Pick 1-2 specializations
    specs = random.sample(list(SKILL_POOLS.keys()), k=random.randint(1, 2))
    skills = []
    for s in specs:
        skills.extend(random.sample(SKILL_POOLS[s], k=random.randint(3, min(7, len(SKILL_POOLS[s])))))
    skills = list(set(skills))

    titles = []
    for s in specs:
        titles.extend(random.sample(TITLE_POOLS[s], k=random.randint(1, 2)))

    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}@{random.choice(['gmail.com', 'outlook.com', 'yahoo.com', 'protonmail.com', 'company.com'])}"
    years = random.randint(1, 25)
    edu = random.sample(EDUCATION, k=random.randint(1, 2))
    num_jobs = min(random.randint(1, 4), max(1, years // 3))
    companies = random.sample(COMPANIES, k=num_jobs)

    # Assign clearance — higher chance for security/devops specs or defense companies
    clearance = ""
    has_defense = any(c in CLEARED_COMPANIES for c in companies)
    has_sec_spec = "security" in specs or "devops" in specs
    if has_defense:
        clearance = random.choice(CLEARANCE_LEVELS)
    elif has_sec_spec and random.random() < 0.4:
        clearance = random.choice(CLEARANCE_LEVELS)
    elif random.random() < 0.1:
        clearance = random.choice(CLEARANCE_LEVELS[2:])  # Secret or below

    # Build resume text
    sections = {}
    sections["header"] = f"{name}\n{email}\n{random.choice(['San Francisco', 'New York', 'Seattle', 'Austin', 'Chicago', 'Denver', 'Boston', 'Portland', 'Atlanta', 'Miami', 'Toronto', 'London', 'Berlin', 'Singapore', 'Bangalore', 'Remote'])}, {random.choice(['CA', 'NY', 'WA', 'TX', 'IL', 'CO', 'MA', 'OR', 'GA', 'FL', 'ON', 'UK', 'DE', 'SG', 'IN', ''])}"

    sections["summary"] = f"Summary\n{titles[0]} with {years} years of experience specializing in {', '.join(skills[:3])}. Proven track record of delivering scalable solutions and driving technical excellence."

    exp_lines = ["Experience"]
    for i, company in enumerate(companies):
        title = titles[i % len(titles)]
        exp_lines.append(f"\n{title} at {company}")
        exp_lines.append(f"{random.randint(max(2000, 2024 - years), 2024)} - {'Present' if i == 0 else str(random.randint(2020, 2025))}")
        for _ in range(random.randint(2, 4)):
            exp_lines.append(f"- {_rand_achievement()}")
    sections["experience"] = "\n".join(exp_lines)

    sections["skills"] = "Skills\n" + ", ".join(skills)

    if clearance:
        sections["clearance"] = f"Security Clearance\n{clearance} — Active"

    edu_lines = ["Education"]
    for e in edu:
        edu_lines.append(f"{e['degree']} - {e['school']}")
    sections["education"] = "\n".join(edu_lines)

    full_text = "\n\n".join(sections.values())

    return {
        "name": name,
        "email": email,
        "skills": skills,
        "titles": titles,
        "years_experience": years,
        "clearance": clearance,
        "education": edu,
        "text": full_text,
        "sections": sections,
    }


async def run_simulation(count: int = 10000) -> AsyncGenerator[str, None]:
    """Generate and ingest mock resumes, yielding SSE progress events."""
    mc = get_minio()
    ensure_bucket(mc)
    BUCKET = "resumes"

    batch_size = 50
    ingested = 0
    errors = 0

    yield f"data: {json.dumps({'status': 'starting', 'total': count})}\n\n"

    for batch_start in range(0, count, batch_size):
        batch_end = min(batch_start + batch_size, count)
        batch_candidates = []

        for _ in range(batch_start, batch_end):
            candidate = generate_candidate()
            candidate_id = str(uuid.uuid4())
            batch_candidates.append((candidate_id, candidate))

        for candidate_id, candidate in batch_candidates:
            try:
                # Save processed JSON to MinIO
                processed_path = f"processed/{candidate_id}/extracted.json"
                extracted = {
                    "name": candidate["name"],
                    "email": candidate["email"],
                    "skills": candidate["skills"],
                    "titles": candidate["titles"],
                    "years_experience": candidate["years_experience"],
                    "clearance": candidate["clearance"],
                    "education": candidate["education"],
                }
                processed_bytes = json.dumps(extracted, indent=2).encode()
                mc.put_object(BUCKET, processed_path, io.BytesIO(processed_bytes), len(processed_bytes))

                # Insert candidate
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """INSERT INTO candidates
                           (id, name, email, skills, titles, years_experience, clearance,
                            education, raw_file_path, processed_file_path)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            candidate_id,
                            candidate["name"],
                            candidate["email"],
                            candidate["skills"],
                            candidate["titles"],
                            candidate["years_experience"],
                            candidate["clearance"],
                            json.dumps(candidate["education"]),
                            f"simulated/{candidate_id}",
                            processed_path,
                        ),
                    )

                    # Embed and store chunks
                    for section_name, section_text in candidate["sections"].items():
                        if not section_text.strip():
                            continue
                        embedding = embed_text(section_text[:2000])
                        cur.execute(
                            """INSERT INTO resume_chunks
                               (candidate_id, chunk_text, embedding, section)
                               VALUES (%s, %s, %s::vector, %s)""",
                            (candidate_id, section_text, str(embedding), section_name),
                        )
                    conn.commit()

                ingested += 1
            except Exception as e:
                errors += 1

        yield f"data: {json.dumps({'status': 'progress', 'ingested': ingested, 'errors': errors, 'total': count})}\n\n"

    yield f"data: {json.dumps({'status': 'complete', 'ingested': ingested, 'errors': errors, 'total': count})}\n\n"
