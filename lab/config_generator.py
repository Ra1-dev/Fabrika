"""
config_generator.py

Generates topic_config.json automatically from a topic name using Claude API.
Claude acts as a market research expert and produces subreddits, search queries,
competitor lists, pain point keywords, and classification categories specific to
any niche — in seconds, at near-zero cost (~$0.01 per generation).

Usage:
    py config_generator.py --topic "online learning and career roadmaps"
    py config_generator.py --topic "personal finance for Gen Z"
    py config_generator.py --topic "fitness and nutrition science"
    py config_generator.py --topic "UX design careers" --output lab/topic_config.json
    py config_generator.py --topic "digital marketing" --preview

Arguments:
    --topic     Required. The niche you want to analyze.
    --output    Optional. Path to save the config. Default: lab/topic_config.json
    --preview   Optional flag. If set, prints the config to console without saving.
"""

import argparse
import json
import os
import sys
import anthropic
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "stage1/processing/.env"))

SYSTEM_PROMPT = """You are a market research expert who specializes in understanding online communities,
discourse patterns, and where people discuss frustrations and aspirations related to learning and careers.

Your job is to generate precise scraping configurations that will surface the most insightful
public discussions about a given topic — especially posts where people express pain points,
confusion, desires for better solutions, or celebrate breakthroughs.

Always think about: where do frustrated beginners go? Where do people share success stories?
Where do they complain about existing products? Where do they ask "where do I even start?"
"""

GENERATION_PROMPT = """Generate a Reddit and YouTube scraping configuration for the following research topic:

TOPIC: {topic}

Your goal is to find online discourse that reveals:
1. What pain points and frustrations people have in this space
2. What existing solutions people are comparing or complaining about
3. What success stories look like and what made them succeed
4. What questions beginners and career-changers ask most
5. What content goes viral and why

Return ONLY a valid JSON object with exactly this structure — no markdown, no explanation, no code fences:

{{
  "topic": "{topic}",
  "description": "<one sentence: what kind of discourse are we looking for and what insight we want to extract>",

  "reddit": {{
    "subreddits": [
      "<list of 12-15 subreddits where this topic is actively discussed — mix of large general ones and small specific ones>"
    ],
    "search_queries": [
      "<list of 10-14 search terms — focus on phrases frustrated people or curious beginners actually type, not marketing language>"
    ],
    "max_items": 500
  }},

  "youtube": {{
    "search_queries": [
      "<list of 12-15 YouTube search queries — mix of 'how to', 'roadmap', 'I became a X', 'best way to learn X', comparison queries>"
    ],
    "max_results_per_query": 10
  }},

  "nlp": {{
    "competitors": [
      "<list of 10-15 existing products, platforms, tools, or resources in this space that people compare or complain about>"
    ],
    "keywords_of_interest": [
      "<list of 10-15 words or short phrases that signal pain points, confusion, or desire for better solutions>"
    ]
  }},

  "llm_classification": {{
    "content_categories": [
      "<list of exactly 8 content categories relevant to this specific topic — e.g. frustration, success_story, question, recommendation, comparison, tutorial, news, meme>"
    ],
    "growth_types": ["organic", "reactive", "educational", "competitive"],
    "audiences": [
      "<list of exactly 5 audience segments most relevant to this topic>"
    ],
    "custom_prompt_context": "<one sentence giving the LLM classifier context about what to focus on when reading posts in this niche>"
  }}
}}"""


def generate_config(topic: str) -> dict:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not found in .env file.")
        print("Make sure lab/stage1/processing/.env contains ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    print(f"Generating config for: '{topic}'")
    print("Calling Claude API...")

    message = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=2000,
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": GENERATION_PROMPT.format(topic=topic)
        }]
    )

    raw = message.content[0].text.strip()

    # Strip markdown fences if the model wrapped output despite instructions
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        config = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERROR: Claude returned invalid JSON: {e}")
        print("Raw response:")
        print(raw)
        sys.exit(1)

    # Validate required keys exist
    required_keys = ["topic", "description", "reddit", "youtube", "nlp", "llm_classification"]
    for key in required_keys:
        if key not in config:
            print(f"ERROR: Generated config missing required key: '{key}'")
            sys.exit(1)

    return config


def print_summary(config: dict):
    print("\nGenerated config summary:")
    print(f"  Topic: {config['topic']}")
    print(f"  Description: {config['description']}")
    print(f"  Subreddits ({len(config['reddit']['subreddits'])}): {', '.join(config['reddit']['subreddits'][:5])}...")
    print(f"  Reddit queries ({len(config['reddit']['search_queries'])}): {config['reddit']['search_queries'][0]}, ...")
    print(f"  YouTube queries ({len(config['youtube']['search_queries'])}): {config['youtube']['search_queries'][0]}, ...")
    print(f"  Competitors tracked ({len(config['nlp']['competitors'])}): {', '.join(config['nlp']['competitors'][:4])}...")
    print(f"  Pain point keywords ({len(config['nlp']['keywords_of_interest'])}): {', '.join(config['nlp']['keywords_of_interest'][:4])}...")
    print(f"  Audiences: {', '.join(config['llm_classification']['audiences'])}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate topic_config.json from a topic name using Claude API"
    )
    parser.add_argument(
        "--topic",
        required=True,
        help='The niche to generate config for. Example: "online learning and career roadmaps"'
    )
    parser.add_argument(
        "--output",
        default=os.path.join(os.path.dirname(__file__), "topic_config.json"),
        help="Output path for the config file (default: lab/topic_config.json)"
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Print config to console without saving to file"
    )
    args = parser.parse_args()

    # Check if config already exists and confirm overwrite
    output_path = os.path.abspath(args.output)
    if not args.preview and os.path.exists(output_path):
        answer = input(f"\ntopic_config.json already exists at {output_path}.\nOverwrite it? (y/n): ").strip().lower()
        if answer != "y":
            print("Aborted. Existing config was not changed.")
            sys.exit(0)

    config = generate_config(args.topic)
    print_summary(config)

    if args.preview:
        print("\nFull config:")
        print(json.dumps(config, indent=2, ensure_ascii=False))
    else:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        print(f"\nConfig saved to: {output_path}")
        print("\nNext steps:")
        print("  1. Review the config if needed:  notepad lab/topic_config.json")
        print("  2. Run the Reddit scraper:        cd lab/stage1/scrapers/reddit && py reddit_scraper.py")
        print("  3. Run the YouTube scraper:       cd lab/stage1/scrapers/youtube && py youtube_scraper_v2.py")
        print("  4. Run the pipeline:              cd lab/stage1/processing && py run_pipeline.py")


if __name__ == "__main__":
    main()
