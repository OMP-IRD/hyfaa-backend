"""General page routes."""
from flask import Blueprint
from flask import current_app as app
from flask import make_response, render_template
from flask import jsonify

# Blueprint Configuration
handlers_bp = Blueprint(
    "handlers_bp", __name__, template_folder="templates", static_folder="static"
)

@app.errorhandler(404)
def not_found(e):
    """Page not found."""
    return make_response(
        render_template("404.html"),
        404
     )

@app.errorhandler(500)
def server_error(e):
    """Internal server error."""
    return make_response(
        render_template("500.html"),
        500
    )

