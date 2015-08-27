package com.velankani.apps.connectxml

import scalaj.http._

/**
 * This class checks a webpage for emails and phone numbers. It just gets the
 * entire HTML of the page and then parses it, as other more advanced parsers
 * are not serializable and thus cannot be used in a spark map() function
 */
class ScalaScraper(url: String) extends Serializable{
	
	var doc = Http(url).asString
	var pText = doc.toString

	/**
	 * Method that gets all the phone numbers inide the body of the page
	 * Output: A list of strings, where each string represents a phone number
	 *
	 */
	def getPhones(): List[String] = {
		var toReturn: List[String] = List()
		val regex = "^(?:(?:\\+?1\\s*(?:[.-]\\s*)?)?(?:\\(\\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\\s*\\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\\s*(?:[.-]\\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\\s*(?:[.-]\\s*)?([0-9]{4})(?:\\s*(?:#|x\\.?|ext\\.?|extension)\\s*(\\d+))?$"
		val noPunct = pText.replaceAll("""[\p{Punct}]""", "")
		val tokens = noPunct.split("\\s+")
		for (t <- tokens) {
			if (t.matches(regex)) {
				toReturn = toReturn :+ t
			}
		}
		return toReturn.distinct
	}

	/**
	 * Method that gets all the phone numbers inide the body of the page
	 * Output: A list of strings, where each string represents a phone number
	 *
	 */
	def getEmails(): List[String] = {
		var toReturn: List[String] = List()
		val regex = "[a-zA-Z0-9_.]+@[a-z0-9\\-]+\\.[a-z]+(\\.[a-z]+)?"
		val tokens = pText.split("\\s+")
		for (t <- tokens) {
			if (t.matches(regex)) {
				toReturn = toReturn :+ t
			}
		}
		return toReturn.distinct
	}

	/**
	 * Allows you to reuse this class instead of continuously making new instances
	 */
	def changeUrl(url2: String) = {
		doc = Http(url2).asString
		pText = doc.toString
	}
}