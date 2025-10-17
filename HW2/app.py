#!/usr/bin/env python3
"""
Flask web application for the search engine frontend.
"""

import os
import time
import json
from flask import Flask, render_template, request, jsonify, send_from_directory
from engine.searcher import Searcher
from engine.paths import LEXICON_PATH, POSTINGS_PATH, DOC_LENGTHS_PATH

app = Flask(__name__)

# Global searcher instance
searcher = None
doc_collection = {}

# def get_document_content(docid):
#     """Get document content by ID from the collection file."""
#     collection_path = "data/collection.tsv"
    
#     if not os.path.exists(collection_path):
#         return "Document content not available"
    
#     try:
#         with open(collection_path, 'r', encoding='utf-8') as f:
#             for line in f:
#                 parts = line.strip().split('\t', 1)
#                 if len(parts) == 2 and int(parts[0]) == docid:
#                     return parts[1]
#         return "Document not found"
#     except Exception as e:
#         return f"Error loading document: {str(e)}"

def initialize_searcher():
    """Initialize the search engine."""
    global searcher
    try:
        print("Initializing search engine...")
        searcher = Searcher(
            lexicon_path=LEXICON_PATH,
            postings_path=POSTINGS_PATH,
            doc_lengths=DOC_LENGTHS_PATH
        )
        print("Search engine initialized successfully")
    except Exception as e:
        print(f"Error initializing search engine: {e}")
        searcher = None

@app.route('/')
def index():
    """Serve the main search page."""
    return send_from_directory('frontend', 'index.html')

@app.route('/search', methods=['POST'])
def search():
    """Handle search requests."""
    if searcher is None:
        return jsonify({'error': 'Search engine not initialized'}), 500
    
    try:
        data = request.get_json()
        query = data.get('query', '').strip()
        mode = data.get('mode', 'AND').upper()
        
        if not query:
            return jsonify({'error': 'Empty query'}), 400
        
        if mode not in ['AND', 'OR']:
            return jsonify({'error': 'Invalid mode. Must be AND or OR'}), 400
        
        # Perform search with timing
        start_time = time.perf_counter()
        results = searcher.search_topk_daat(query, mode=mode, topk=10)
        end_time = time.perf_counter()
        
        search_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # Format results
        formatted_results = []
        for result in results:
            if isinstance(result, tuple) and len(result) == 2:
                # Ranked results: (docid, score)
                docid, score = result
                # content = get_document_content(docid)
                # snippet = create_snippet(content, query)
                formatted_results.append({
                    'docid': docid,
                    'score': score,
                    # 'content': content,
                    # 'snippet': snippet
                })
            else:
                # Boolean results: just docid
                docid = result
                # content = get_document_content(docid)
                # snippet = create_snippet(content, query)
                formatted_results.append({
                    'docid': docid,
                    'score': None,
                    # 'content': content,
                    # 'snippet': snippet
                })
        
        return jsonify({
            'results': formatted_results,
            'searchTime': search_time,
            'totalResults': len(formatted_results),
            'query': query,
            'mode': mode
        })
        
    except Exception as e:
        print(f"Search error: {e}")
        return jsonify({'error': f'Search failed: {str(e)}'}), 500

def create_snippet(content, query, max_length=200):
    """Create a snippet highlighting query terms."""
    if not content or not query:
        return content[:max_length] + "..." if len(content) > max_length else content
    
    query_terms = query.lower().split()
    content_lower = content.lower()
    
    # Find the first occurrence of any query term
    best_start = 0
    best_score = 0
    
    for term in query_terms:
        pos = content_lower.find(term)
        if pos != -1:
            # Calculate a score based on how many query terms are nearby
            nearby_terms = 0
            for other_term in query_terms:
                if other_term in content_lower[max(0, pos-50):pos+50]:
                    nearby_terms += 1
            
            if nearby_terms > best_score:
                best_score = nearby_terms
                best_start = max(0, pos - 50)
    
    # Extract snippet around the best position
    snippet_start = best_start
    snippet_end = min(len(content), snippet_start + max_length)
    
    snippet = content[snippet_start:snippet_end]
    
    # Add ellipsis if needed
    if snippet_start > 0:
        snippet = "..." + snippet
    if snippet_end < len(content):
        snippet = snippet + "..."
    
    return snippet

@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'searcher_initialized': searcher is not None
    })

if __name__ == '__main__':
    # Initialize the search engine
    initialize_searcher()
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5001)
