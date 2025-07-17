from src.graph.state import AgentState, show_agent_reasoning
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage
from pydantic import BaseModel
import json
from typing_extensions import Literal
from src.tools.financial_metrics_service import get_financial_metrics
from src.tools.price_service import get_price_data
from src.tools.company_facts_service import get_market_cap
from src.utils.llm import call_llm
from src.utils.progress import progress
from typing import Optional
from src.llm.models import get_model, get_model_info

# System prompt for Sophie analysis
SOPHIE_SYSTEM_PROMPT = """You are Sophie, an AI investment analyst that combines multiple analysis techniques:
- Valuation analysis (DCF, ev_ebitda, owner_earnings, residual_income)
- Technical analysis (trend, momentum, volatility) 
- Fundamental analysis (financial statements)
- Sentiment analysis (news, social media)

Provide an analysis with:
1. Overall score (1-100) where:
   - 1-20 = Strong Sell
   - 21-40 = Sell
   - 41-60 = Hold  
   - 61-80 = Buy
   - 81-100 = Strong Buy
2. Confidence level (0-100%)
3. Time horizon specific insights (short/medium/long term)
4. Key bullish factors
5. Key bearish factors
6. Potential risks to the analysis

Return analysis in this JSON format:
{{
  "signal": "bullish"|"bearish"|"neutral",
  "confidence": 0-100,
  "overall_score": 1-100,
  "reasoning": "Analysis rationale",
  "time_horizon_analysis": {{
    "short_term": "1-3 month outlook",
    "medium_term": "3-12 month outlook", 
    "long_term": "1+ year outlook"
  }},
  "bullish_factors": ["list", "of", "factors"],
  "bearish_factors": ["list", "of", "factors"],
  "risks": ["potential", "risks", "to", "analysis"]
}}"""

class SophieSignal(BaseModel):
    signal: Literal["bullish", "bearish", "neutral"]
    confidence: float  # 0-1
    overall_score: int  # 1-100
    reasoning: str
    time_horizon_analysis: dict[str, str]  # short/medium/long term
    bullish_factors: list[str] = []
    bearish_factors: list[str] = []
    risks: list[str] = []

def sophie_agent(state: AgentState):
    """Analyzes stocks using combined valuation, technical, sentiment and fundamental analysis."""
    data = state["data"]
    tickers = data["tickers"]

    analysis_data = {}
    sophie_analysis = {}

    for ticker in tickers:
        progress.update_status("sophie_agent", ticker, "Collecting analysis data")
        
        # Get valuation data
        valuation = get_valuation_analysis(ticker)
        
        # Get technical analysis
        technicals = get_technical_analysis(ticker)
        
        # Get sentiment analysis
        sentiment = get_sentiment_analysis(ticker)
        
        # Get fundamental analysis
        fundamentals = get_fundamental_analysis(ticker)

        # Combine all analyses
        analysis_data[ticker] = {
            "valuation": valuation,
            "technicals": technicals,
            "sentiment": sentiment,
            "fundamentals": fundamentals
        }

        # Debug log the analysis data structure
        print(f"Analysis data for {ticker}:")
        print(json.dumps(analysis_data[ticker], indent=2))

        progress.update_status("sophie_agent", ticker, "Generating LLM analysis")
        sophie_output = generate_llm_output(
            ticker=ticker,
            analysis_data=analysis_data,
            model_name=state["metadata"]["model_name"],
            model_provider=state["metadata"]["model_provider"],
        )

        # Store analysis
        sophie_analysis[ticker] = {
            "signal": sophie_output.signal,
            "confidence": sophie_output.confidence,
            "overall_score": sophie_output.overall_score,
            "reasoning": sophie_output.reasoning,
            "time_horizon_analysis": sophie_output.time_horizon_analysis,
            "bullish_factors": sophie_output.bullish_factors,
            "bearish_factors": sophie_output.bearish_factors,
            "risks": sophie_output.risks
        }

        progress.update_status("sophie_agent", ticker, "Done")

    # Create the message
    message = HumanMessage(
        content=json.dumps(sophie_analysis),
        name="sophie_agent"
    )

    # Show reasoning if requested
    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(sophie_analysis, "Sophie Agent")

    # Add the signal to the analyst_signals list
    state["data"]["analyst_signals"]["sophie_agent"] = sophie_analysis

    return {"messages": [message], "data": state["data"]}

def get_valuation_analysis(ticker: str) -> dict:
    """Get valuation analysis from database including all methods and weighted result"""
    from src.tools.api_db import get_valuation_db
    from datetime import datetime
    
    # Get latest valuation data
    today = datetime.now().strftime('%Y-%m-%d')
    results = get_valuation_db(ticker, today)
    
    if not results:
        return {"error": "No valuation data available"}
    
    # Convert all results to serializable format
    def decimal_to_float(value):
        if value is None:
            return None
        return float(value)
    
    valuations = []
    for result in results:
        valuations.append({
            "valuation_method": result['valuation_method'],
            "intrinsic_value": decimal_to_float(result['intrinsic_value']),
            "market_cap": decimal_to_float(result['market_cap']),
            "gap": decimal_to_float(result['gap']),
            "signal": result['signal']
        })
    
    return {"valuations": valuations}

def get_technical_analysis(ticker: str) -> dict:
    """Get latest technical analysis from database"""
    from src.tools.api_db import get_technicals_db
    from datetime import datetime
    
    # Get latest technical data
    today = datetime.now().strftime('%Y-%m-%d')
    results = get_technicals_db(ticker, today, 1)
    
    if not results:
        return {"error": "No technical data available"}
    
    result = results[0]
    
    # Prepare strategies data
    strategies = [
        {
            "strategy": "trend",
            "signal": result["trend_signal"],
            "confidence": float(result["trend_confidence"])
        },
        {
            "strategy": "mean_reversion",
            "signal": result["mr_signal"],
            "confidence": float(result["mr_confidence"])
        },
        {
            "strategy": "momentum",
            "signal": result["momentum_signal"],
            "confidence": float(result["momentum_confidence"])
        },
        {
            "strategy": "volatility",
            "signal": result["volatility_signal"],
            "confidence": float(result["volatility_confidence"])
        },
        {
            "strategy": "stat_arb",
            "signal": result["stat_arb_signal"],
            "confidence": float(result["stat_arb_confidence"])
        }
    ]
    
    return {
        "composite_signal": result["signal"],
        "composite_confidence": float(result["confidence"]) if result["confidence"] else None,
        "strategies": strategies
    }

def get_sentiment_analysis(ticker: str) -> dict:
    """Get sentiment analysis from database"""
    from src.tools.api_db import get_sentiment_db
    from datetime import datetime
    
    # Get latest sentiment data
    today = datetime.now().strftime('%Y-%m-%d')
    results = get_sentiment_db(ticker, today, 1)
    
    if not results:
        return {"error": "No sentiment data available"}
    
    result = results[0]
    
    return {
        "overall_signal": result["overall_signal"],
        "confidence": float(result["confidence"]),
        "insider_trading": {
            "total": result["insider_total"],
            "bullish": result["insider_bullish"],
            "bearish": result["insider_bearish"],
            "total_value": float(result["insider_value_total"]),
            "bullish_value": float(result["insider_value_bullish"]),
            "bearish_value": float(result["insider_value_bearish"]),
            "weight": float(result["insider_weight"])
        },
        "news_sentiment": {
            "total": result["news_total"],
            "bullish": result["news_bullish"],
            "bearish": result["news_bearish"],
            "neutral": result["news_neutral"],
            "weight": float(result["news_weight"])
        }
    }

def get_fundamental_analysis(ticker: str) -> dict:
    """Get fundamental analysis from database"""
    from src.tools.api_db import get_fundamentals_db
    from datetime import datetime
    
    # Get latest fundamentals data
    today = datetime.now().strftime('%Y-%m-%d')
    results = get_fundamentals_db(ticker, today, 1)
    
    if not results:
        return {"error": "No fundamental data available"}
    
    result = results[0]
    
    return {
        "overall_signal": result["overall_signal"],
        "confidence": float(result["confidence"]),
        "profitability": {
            "return_on_equity": float(result["return_on_equity"]),
            "net_margin": float(result["net_margin"]),
            "operating_margin": float(result["operating_margin"]),
            "signal": result["profitability_signal"]
        },
        "growth": {
            "revenue_growth": float(result["revenue_growth"]),
            "earnings_growth": float(result["earnings_growth"]),
            "book_value_growth": float(result["book_value_growth"]),
            "signal": result["growth_signal"]
        },
        "financial_health": {
            "current_ratio": float(result["current_ratio"]) if result["current_ratio"] is not None else None,
            "debt_to_equity": float(result["debt_to_equity"]) if result["debt_to_equity"] is not None else None,
            "free_cash_flow_per_share": float(result["free_cash_flow_per_share"]) if result["free_cash_flow_per_share"] is not None else None,
            "signal": result["health_signal"]
        },
        "valuation": {
            "pe_ratio": float(result["pe_ratio"]),
            "pb_ratio": float(result["pb_ratio"]),
            "ps_ratio": float(result["ps_ratio"]),
            "signal": result["valuation_signal"]
        }
    }

def save_prompt_to_log(ticker: str):
    """Save complete LLM prompt to log file with latest real DB data"""
    import os
    from datetime import datetime
    
    # Get latest analysis data
    valuation = get_valuation_analysis(ticker)
    technicals = get_technical_analysis(ticker)
    sentiment = get_sentiment_analysis(ticker)
    fundamentals = get_fundamental_analysis(ticker)
    
    # Create log directory if needed
    os.makedirs("logs", exist_ok=True)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"logs/{ticker}_analysis_{timestamp}.txt"
    
    # Create the full prompt template with all components
    template = """### Investment Analysis Prompt Template

**System Instructions:**
You are Sophie, an AI investment analyst that combines multiple analysis techniques:
- Valuation analysis (DCF, ev_ebitda, owner_earnings, residual_income)
- Technical analysis (trend, momentum, volatility)
- Fundamental analysis (financial statements)
- Sentiment analysis (news, social media)

**Analysis Requirements:**
1. Provide an overall score (1-100) where:
   - 1-20 = Strong Sell
   - 21-40 = Sell 
   - 41-60 = Hold
   - 61-80 = Buy
   - 81-100 = Strong Buy
2. Confidence level (0-100%) in your assessment
3. Time horizon specific insights (short/medium/long term)
4. Key bullish factors
5. Key bearish factors
6. Potential risks to the analysis

**Required Output Format:**
```json
{{
  "confidence": 0-100,
  "overall_score": 1-100,
  "reasoning": "Analysis rationale",
  "time_horizon_analysis": {{
    "short_term": "1-3 month outlook",
    "medium_term": "3-12 month outlook",
    "long_term": "1+ year outlook"
  }},
  "bullish_factors": ["list", "of", "factors"],
  "bearish_factors": ["list", "of", "factors"],
  "risks": ["potential", "risks", "to", "analysis"]
}}
```

**Stock Analysis Request:**
Analyze {ticker} based on the following data:

=== Valuation Analysis ===
{valuation_data}

=== Technical Analysis ===  
{technical_data}

=== Sentiment Analysis ===
{sentiment_data}

=== Fundamental Analysis ===
{fundamentals_data}

=== Additional Notes ===
- Consider sector/industry trends
- Account for macroeconomic conditions
- Highlight any unusual data points
- Tone of financial analyst"""
    
    # Format prompt content with actual data
    content = template.format(
        ticker=ticker,
        valuation_data=json.dumps(valuation, indent=2),
        technical_data=json.dumps(technicals, indent=2),
        sentiment_data=json.dumps(sentiment, indent=2),
        fundamentals_data=json.dumps(fundamentals, indent=2)
    )
    
    # Write to file
    with open(filename, "w") as f:
        f.write(content)
    
    return filename

def run_analysis(ticker: str):
    """Run analysis and save prompt to log file"""
    filename = save_prompt_to_log(ticker)
    print(f"Analysis saved to: {filename}")
    return filename

def generate_llm_output_direct(
    ticker: str,
    analysis_data: dict[str, any],
    model_name: str,
    model_provider: str,
) -> SophieSignal:
    """Direct LLM output generation without saving to log"""
    def create_default_sophie_signal():
        return SophieSignal(
            signal="neutral",
            confidence=0.0,
            overall_score=50,
            reasoning="Error in analysis, defaulting to neutral",
            time_horizon_analysis={
                "short_term": "Unknown",
                "medium_term": "Unknown", 
                "long_term": "Unknown"
            },
            bullish_factors=[],
            bearish_factors=[],
            risks=[]
        )

    # Validate analysis data exists for this ticker
    if ticker not in analysis_data:
        print(f"No analysis data found for {ticker}")
        return create_default_sophie_signal()

    try:
        # Create template with validation
        template = ChatPromptTemplate.from_messages([
            ("system", SOPHIE_SYSTEM_PROMPT),
            ("human", "Analyze {ticker} based on this data:\n{analysis_data}")
        ])

        if not template:
            raise ValueError("Failed to create prompt template")

        prompt = template.invoke({
            "ticker": ticker,
            "analysis_data": json.dumps(analysis_data[ticker], indent=2)
        })

        if not prompt:
            raise ValueError("Failed to generate prompt")

        return call_llm(
            prompt=prompt,
            model_name=model_name,
            model_provider=model_provider,
            pydantic_model=SophieSignal,
            agent_name="sophie_agent",
            default_factory=create_default_sophie_signal
        )
    except Exception as e:
        print(f"Error in direct generation: {e}")
        return create_default_sophie_signal()

def generate_llm_output(
    ticker: str,
    analysis_data: dict[str, any],
    model_name: str,
    model_provider: str,
) -> SophieSignal:
    """Generate analysis output by calling the LLM"""
    # First try direct generation which is more reliable
    try:
        return generate_llm_output_direct(ticker, analysis_data, model_name, model_provider)
    except Exception as e:
        print(f"Direct generation failed: {e}")
        # Fallback to log-based approach if direct fails
        try:
            prompt_content = save_prompt_to_log(ticker)
            if not prompt_content:
                raise ValueError("Failed to generate prompt content")
            
            template = ChatPromptTemplate.from_messages([
                ("system", SOPHIE_SYSTEM_PROMPT),
                ("human", "Analyze {ticker} based on this data:\n{analysis_data}")
            ])
            
            if not template:
                raise ValueError("Failed to create prompt template")
                
            prompt = template.invoke({
                "ticker": ticker,
                "analysis_data": json.dumps(analysis_data[ticker], indent=2)
            })
            
            return call_llm(
                prompt=prompt,
                model_name=model_name,
                model_provider=model_provider,
                pydantic_model=SophieSignal,
                agent_name="sophie_agent",
                default_factory=lambda: SophieSignal(
                    signal="neutral",
                    confidence=0.0,
                    overall_score=50,
                    reasoning="Error in analysis, defaulting to neutral",
                    time_horizon_analysis={
                        "short_term": "Unknown",
                        "medium_term": "Unknown", 
                        "long_term": "Unknown"
                    },
                    bullish_factors=[],
                    bearish_factors=[],
                    risks=[]
                )
            )
        except Exception as e:
            print(f"Log-based generation failed: {e}")
            # Final fallback to default neutral signal
            return SophieSignal(
                signal="neutral",
                confidence=0.0,
                overall_score=50,
                reasoning=f"Analysis failed: {str(e)}",
                time_horizon_analysis={
                    "short_term": "Unknown",
                    "medium_term": "Unknown", 
                    "long_term": "Unknown"
                },
                bullish_factors=[],
                bearish_factors=[],
                risks=[]
            )
    
    # If we reach here, all attempts failed - return default signal
    return SophieSignal(
        signal="neutral",
        confidence=0.0,
        overall_score=50,
        reasoning="All analysis methods failed",
        time_horizon_analysis={
            "short_term": "Unknown",
            "medium_term": "Unknown", 
            "long_term": "Unknown"
        },
        bullish_factors=[],
        bearish_factors=[],
        risks=[]
    )
