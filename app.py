from flask import Flask, render_template, make_response, Response, abort, request, copy_current_request_context
from flask_mail import Mail, Message
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from werkzeug.middleware.proxy_fix import ProxyFix
from user_agents import parse
from datetime import datetime
from dataController import DataController
from dotenv import load_dotenv
import os
import threading

# enviorment variables (my special secrets)
load_dotenv()
DATABASE_PATH = os.getenv("DATABASE_PATH")
PROXY_COUNT = os.getenv("PROXY_COUNT")
GMAIL_APP_PASSWRD = os.getenv("GMAIL_APP_PASSWRD")
GMAIL_USERNAME = os.getenv("GMAIL_USERNAME")
SPAM_LIMIT = os.getenv("SPAM_LIMIT")

# app configuration and modules
app = Flask(__name__)
app.wsgi_app = ProxyFix(
    app.wsgi_app, 
    x_for=PROXY_COUNT,
    x_proto=PROXY_COUNT,
    x_host=PROXY_COUNT
)
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = GMAIL_USERNAME
app.config['MAIL_PASSWORD'] = GMAIL_APP_PASSWRD
mail = Mail(app)
limiter = Limiter(get_remote_address, app=app)
siteData = DataController(DATABASE_PATH)

# main routes
@app.route('/')
def home():
    return render_template("index.html", hero=True, home=True, pageTitle='Calvin Gozé')

@app.route('/blog/<blogposturl>')
def blogdetails(blogposturl):
    blogPost = siteData.getBlogPost(blogposturl)
    if not blogPost:
        abort(404)
    return render_template("blogdetails.html", post=blogPost, pageTitle='{} | Blog | Calvin Gozé'.format(blogPost['title']))

@app.route('/blog')
def blog():
    blogPosts = siteData.getBlogPosts()
    return render_template("blog.html", posts=blogPosts, hero=True, pageTitle='Blog | Calvin Gozé')

@app.route('/terms-of-use')
def termsofuse():
    return render_template("termsofuse.html", pageTitle='Terms of Use | Calvin Gozé')

@app.route('/privacy')
def privacy():
    return render_template("privacy.html", pageTitle='Privacy Policy | Calvin Gozé')

@app.route('/contact', methods=['GET', 'POST'])
@limiter.limit(SPAM_LIMIT, methods=['POST'])
def contact():
    if request.method == 'POST':
        if request.form.get('honeypot'):
            return "", 200
        name = request.form.get('name')
        email = request.form.get('email')
        message = request.form.get('message')
        ip = request.remote_addr
        user_agent = parse(request.headers.get('User-Agent'))
        device_info = {
            "browser": user_agent.browser.family,
            "os": user_agent.os.family,
            "device": "Mobile" if user_agent.is_mobile else "Tablet" if user_agent.is_tablet else "PC",
            "is_touch_capable": user_agent.is_touch_capable
        }
        siteData.insertMessage(name,email,message,ip,str(device_info))
        
        # email in a seperate thread, otherwise, the page sits for ~3 seconds before reloading
        @copy_current_request_context
        def sendEmail(name, email, message):
            msg = Message(subject=f"New Contact from {name}",
                    sender=GMAIL_USERNAME,
                    recipients=[GMAIL_USERNAME], # Send to yourself
                    body=f"From: {name} <{email}>\n\n{message}")
            mail.send(msg)

        t1 = threading.Thread(target=sendEmail, args=(name,email,message,))
        t1.start()
        
        return render_template("contact-postresponse.html", pageTitle='Contact | Calvin Gozé')
        
    return render_template("contact.html", pageTitle='Contact | Calvin Gozé')

# routes for crawlers
@app.route('/sitemap.xml')
def sitemap():
    blogPosts = siteData.getBlogPosts()
    xmlContent = render_template("crawlers/sitemap.xml", posts=blogPosts)
    response = make_response(xmlContent)
    response.headers['Content-Type'] = 'application/xml'
    return response

@app.route('/robots.txt')
def robots_txt():
    content = "User-agent: *\nDisallow: /admin/\n\nSitemap: https://calvingoze.com/sitemap.xml"
    return Response(content, mimetype='text/plain')

# error handlers
@app.errorhandler(429)
def ratelimit_handler(e):
    return render_template('errorpages/429.html',pageTitle='Spam Detected'), 429

@app.errorhandler(404)
def page_not_found(error):
    # note that we set the 404 status explicitly
    return render_template('errorpages/404.html',pageTitle='404 Page Not Found'), 404

# templates
@app.template_filter('format_epoch_datetime')
def format_datetime_filter(value, format='%b %d, %Y'):
    if value is None:
        return ""
    timeStamp = datetime.fromtimestamp(value)
    return timeStamp.strftime(format)

if __name__ == '__main__':
    app.run(debug=True)