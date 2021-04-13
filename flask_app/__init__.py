"""Initialize Flask app."""
import os
from flask import Flask
from .apis import api_v1


configurations = {
    "dev": "config.DevelopmentConfig",
    "staging": "config.StagingConfig",
    "prod": "config.ProductionConfig",
    "default": "config.DevelopmentConfig"
}
ENVIRONMENT_OVERRIDES = [
    "DATABASE_URI",
    "STORAGE_PATH",
    "DEFAULT_NEARBY_LIMIT",
]


def configure_app(app):
    """Manage the app's configuration"""
    config_name = os.getenv('FLASK_CONFIGURATION', 'default')
    app.config.from_object(configurations[config_name])
    # Allow defining the source file from environment variable directly (bypassing the config)
    for conf_var in ENVIRONMENT_OVERRIDES:
        val = os.getenv(conf_var)
        if val:
            app.config[conf_var] = val


def init_app():
    """Create Flask application."""
    app = Flask(__name__, instance_relative_config=False)
    configure_app(app)

    with app.app_context():
        # Import parts of our application
        from .apis import blueprint as api
        from .error_handlers import error_handlers

        # Register Blueprints
        app.register_blueprint(api, url_prefix='/api/v1')
        app.register_blueprint(error_handlers.handlers_bp)

        return app

