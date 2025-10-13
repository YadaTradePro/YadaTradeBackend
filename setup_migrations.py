
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to properly set up Alembic migrations for the Flask application.
Run this script to initialize or fix migration issues.
"""

import os
import sys
from flask import Flask
from flask_migrate import Migrate, init, migrate, upgrade, stamp
from extensions import db
from config import Config
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    """Create Flask app for migration operations"""
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # Initialize extensions
    db.init_app(app)
    migrate = Migrate(app, db)
    
    # Import all models to ensure they're registered
    import models
    
    return app

def setup_migrations():
    """Set up Alembic migrations properly"""
    app = create_app()
    
    with app.app_context():
        # Check if migrations directory exists
        migrations_dir = os.path.join(os.getcwd(), 'migrations')
        
        if not os.path.exists(migrations_dir):
            logger.info("Initializing Alembic migrations...")
            init()
            logger.info("✅ Migrations directory created successfully!")
        else:
            logger.info("📁 Migrations directory already exists.")
        
        # Check if database exists and has tables
        try:
            # Get current database state
            from sqlalchemy import inspect
            inspector = inspect(db.engine)
            existing_tables = inspector.get_table_names()
            
            if existing_tables:
                logger.info(f"📋 Found existing tables: {existing_tables}")
                
                # Check if alembic_version table exists
                if 'alembic_version' not in existing_tables:
                    logger.info("🔧 Stamping database with current model state...")
                    # Create alembic_version table and mark as current
                    stamp('head')
                    logger.info("✅ Database stamped successfully!")
                else:
                    logger.info("✅ Alembic version table exists. Database is migration-ready.")
            else:
                logger.info("📊 No existing tables found. Creating initial migration...")
                # Generate initial migration
                migrate(message='Initial migration')
                # Apply the migration
                upgrade()
                logger.info("✅ Initial migration created and applied!")
                
        except Exception as e:
            logger.error(f"❌ Error during migration setup: {e}")
            return False
    
    return True

def check_migration_status():
    """Check current migration status"""
    app = create_app()
    
    with app.app_context():
        try:
            from alembic import command
            from alembic.config import Config as AlembicConfig
            from flask_migrate import current, heads
            
            # Get current revision
            current_rev = current()
            head_revs = heads()
            
            logger.info(f"📍 Current revision: {current_rev}")
            logger.info(f"🎯 Head revision(s): {head_revs}")
            
            if current_rev in head_revs:
                logger.info("✅ Database is up to date!")
                return True
            else:
                logger.warning("⚠️  Database is not up to date. Run migrations.")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error checking migration status: {e}")
            return False

if __name__ == '__main__':
    print("🚀 Setting up Alembic migrations...")
    
    if setup_migrations():
        print("\n🔍 Checking migration status...")
        check_migration_status()
        print("\n✅ Migration setup completed successfully!")
        print("\n📚 Next steps:")
        print("   1. To create a new migration: flask db migrate -m 'Description'")
        print("   2. To apply migrations: flask db upgrade")
        print("   3. To check status: flask db current")
    else:
        print("\n❌ Migration setup failed. Check the logs above.")
        sys.exit(1)
