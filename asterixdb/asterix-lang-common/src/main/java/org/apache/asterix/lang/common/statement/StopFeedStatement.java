package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class StopFeedStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier feedName;

    public StopFeedStatement(Identifier dataverseName, Identifier feedName){
        this.feedName = feedName;
        this.dataverseName = dataverseName;
    }

    @Override
    public byte getKind() {
        return Kind.STOP_FEED;
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getFeedName() {
        return feedName;
    }
}
