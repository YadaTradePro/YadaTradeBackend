
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Comprehensive migration management script for the Flask application.
Handles common migration scenarios and troubleshooting.
"""

import os
import sys
import click
from flask import Flask
from flask_migrate import Migrate, migrate, upgrade, downgrade, current, heads, stamp
from extensions import db
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    db.init_app(app)
    Migrate(app, db)
    
    # Import all models
    import models
    return app

@click.group()
def cli():
    """Database migration management commands"""
    pass

@cli.command()
@click.option('--message', '-m', required=True, help='Migration message')
def create(message):
    """Create a new migration"""
    app = create_app()
    with app.app_context():
        try:
            migrate(message=message)
            logger.info(f"✅ Migration '{message}' created successfully!")
        except Exception as e:
            logger.error(f"❌ Failed to create migration: {e}")

@cli.command()
@click.option('--revision', default='head', help='Revision to upgrade to (default: head)')
def apply(revision):
    """Apply migrations"""
    app = create_app()
    with app.app_context():
        try:
            upgrade(revision=revision)
            logger.info(f"✅ Upgraded to revision: {revision}")
        except Exception as e:
            logger.error(f"❌ Failed to apply migration: {e}")

@cli.command()
def status():
    """Show current migration status"""
    app = create_app()
    with app.app_context():
        try:
            current_rev = current()
            head_revs = heads()
            
            print(f"📍 Current revision: {current_rev or 'None'}")
            print(f"🎯 Head revision(s): {head_revs}")
            
            if current_rev and current_rev in head_revs:
                print("✅ Database is up to date!")
            else:
                print("⚠️  Database needs migration!")
                
        except Exception as e:
            logger.error(f"❌ Error checking status: {e}")

@cli.command()
@click.option('--revision', default='head', help='Revision to stamp (default: head)')
def stamp_db(revision):
    """Stamp database with specific revision"""
    app = create_app()
    with app.app_context():
        try:
            stamp(revision=revision)
            logger.info(f"✅ Database stamped with revision: {revision}")
        except Exception as e:
            logger.error(f"❌ Failed to stamp database: {e}")

@cli.command()
def fix_unicode():
    """Fix Unicode issues in existing database"""
    app = create_app()
    with app.app_context():
        try:
            # Drop and recreate tables if needed for Unicode fix
            logger.info("🔧 Fixing Unicode encoding issues...")
            
            # This is a drastic measure - only use if data can be lost
            confirm = input("⚠️  This will recreate all tables. Continue? (y/N): ")
            if confirm.lower() == 'y':
                db.drop_all()
                upgrade()
                logger.info("✅ Unicode issues fixed!")
            else:
                logger.info("❌ Operation cancelled.")
                
        except Exception as e:
            logger.error(f"❌ Failed to fix Unicode issues: {e}")

@cli.command()
def reset():
    """Reset migrations (dangerous!)"""
    app = create_app()
    with app.app_context():
        try:
            confirm = input("⚠️  This will reset all migrations. Continue? (y/N): ")
            if confirm.lower() == 'y':
                # Remove migration files (keep __pycache__ and env.py)
                import shutil
                versions_dir = os.path.join('migrations', 'versions')
                if os.path.exists(versions_dir):
                    for file in os.listdir(versions_dir):
                        if file.endswith('.py') and file != '__init__.py':
                            os.remove(os.path.join(versions_dir, file))
                
                # Drop all tables and recreate
                db.drop_all()
                migrate(message='Reset migration')
                upgrade()
                logger.info("✅ Migrations reset successfully!")
            else:
                logger.info("❌ Operation cancelled.")
                
        except Exception as e:
            logger.error(f"❌ Failed to reset migrations: {e}")

if __name__ == '__main__':
    cli()
