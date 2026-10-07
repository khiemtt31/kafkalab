import Link from "next/link";
import { siteProfile } from "../config/site-profile";
import { LabIcon } from "./LabIcon";

export function AppFooter() {
  return (
    <footer className="kl-site-footer" aria-label="Application and creator information">
      <div className="kl-site-footer__main">
        <div className="kl-site-footer__brand">
          <Link href="/" className="kl-site-footer__logo"><LabIcon name="flow" /><span>kafka<span>lab</span><b>.</b></span></Link>
          <p>A visual workspace for Kafka logs, delivery, and distributed systems.</p>
        </div>
        <div className="kl-site-footer__creator">
          <span>Created by</span>
          <strong>{siteProfile.creatorName || "Creator details pending"}</strong>
          {siteProfile.creatorBio ? <p>{siteProfile.creatorBio}</p> : null}
          {siteProfile.creatorLinks.length ? <nav aria-label="Creator links">{siteProfile.creatorLinks.map((link) => <a key={link.href} href={link.href} target="_blank" rel="noopener noreferrer">{link.label}</a>)}</nav> : null}
        </div>
        <nav className="kl-site-footer__links" aria-label="Footer navigation">
          <Link href="/guide">Learning guide</Link>
          <Link href="/practice">Interview practice</Link>
          <a href="https://kafka.apache.org/43/" target="_blank" rel="noopener noreferrer">Apache Kafka documentation <span aria-hidden="true">↗</span></a>
        </nav>
      </div>
      <div className="kl-site-footer__bottom"><span>© {new Date().getUTCFullYear()} {siteProfile.appName}.{siteProfile.creatorName ? ` Created by ${siteProfile.creatorName}.` : ""}</span><span>Learn the log. Understand the system.</span></div>
    </footer>
  );
}
