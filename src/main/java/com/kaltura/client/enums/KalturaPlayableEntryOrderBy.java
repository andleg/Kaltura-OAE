package com.kaltura.client.enums;

/**
 * This class was generated using generate.php
 * against an XML schema provided by Kaltura.
 * @date Sun, 19 Jun 11 02:46:50 -0400
 * 
 * MANUAL CHANGES TO THIS CLASS WILL BE OVERWRITTEN.
 */
public enum KalturaPlayableEntryOrderBy {
    PLAYS_ASC ("+plays"),
    PLAYS_DESC ("-plays"),
    VIEWS_ASC ("+views"),
    VIEWS_DESC ("-views"),
    DURATION_ASC ("+duration"),
    DURATION_DESC ("-duration"),
    MS_DURATION_ASC ("+msDuration"),
    MS_DURATION_DESC ("-msDuration"),
    NAME_ASC ("+name"),
    NAME_DESC ("-name"),
    MODERATION_COUNT_ASC ("+moderationCount"),
    MODERATION_COUNT_DESC ("-moderationCount"),
    CREATED_AT_ASC ("+createdAt"),
    CREATED_AT_DESC ("-createdAt"),
    UPDATED_AT_ASC ("+updatedAt"),
    UPDATED_AT_DESC ("-updatedAt"),
    RANK_ASC ("+rank"),
    RANK_DESC ("-rank");

    String hashCode;

    KalturaPlayableEntryOrderBy(String hashCode) {
        this.hashCode = hashCode;
    }

    public String getHashCode() {
        return this.hashCode;
    }

    public static KalturaPlayableEntryOrderBy get(String hashCode) {
        if (hashCode.equals("+plays"))
        {
           return PLAYS_ASC;
        }
        else 
        if (hashCode.equals("-plays"))
        {
           return PLAYS_DESC;
        }
        else 
        if (hashCode.equals("+views"))
        {
           return VIEWS_ASC;
        }
        else 
        if (hashCode.equals("-views"))
        {
           return VIEWS_DESC;
        }
        else 
        if (hashCode.equals("+duration"))
        {
           return DURATION_ASC;
        }
        else 
        if (hashCode.equals("-duration"))
        {
           return DURATION_DESC;
        }
        else 
        if (hashCode.equals("+msDuration"))
        {
           return MS_DURATION_ASC;
        }
        else 
        if (hashCode.equals("-msDuration"))
        {
           return MS_DURATION_DESC;
        }
        else 
        if (hashCode.equals("+name"))
        {
           return NAME_ASC;
        }
        else 
        if (hashCode.equals("-name"))
        {
           return NAME_DESC;
        }
        else 
        if (hashCode.equals("+moderationCount"))
        {
           return MODERATION_COUNT_ASC;
        }
        else 
        if (hashCode.equals("-moderationCount"))
        {
           return MODERATION_COUNT_DESC;
        }
        else 
        if (hashCode.equals("+createdAt"))
        {
           return CREATED_AT_ASC;
        }
        else 
        if (hashCode.equals("-createdAt"))
        {
           return CREATED_AT_DESC;
        }
        else 
        if (hashCode.equals("+updatedAt"))
        {
           return UPDATED_AT_ASC;
        }
        else 
        if (hashCode.equals("-updatedAt"))
        {
           return UPDATED_AT_DESC;
        }
        else 
        if (hashCode.equals("+rank"))
        {
           return RANK_ASC;
        }
        else 
        if (hashCode.equals("-rank"))
        {
           return RANK_DESC;
        }
        else 
        {
           return PLAYS_ASC;
        }
    }
}