package com.ldbc.driver;

import java.util.Map;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.wrapper.CappedGeneratorWrapper;

public abstract class Workload
{
    private boolean isInitialized = false;
    private boolean isCleanedUp = false;

    private long operationCount;
    private long operationStart;
    private long recordCount;

    /**
     * Called once to initialize state for workload
     */
    public final void init( long operationCount, long operationStart, long recordCount, Map<String, String> properties )
            throws WorkloadException
    {
        if ( true == isInitialized )
        {
            throw new WorkloadException( "DB may be initialized only once" );
        }
        isInitialized = true;
        this.operationCount = operationCount;
        this.operationStart = operationStart;
        this.recordCount = recordCount;
        onInit( properties );
    }

    protected long getOperationCount()
    {
        return operationCount;
    }

    protected long getOperationStart()
    {
        return operationStart;
    }

    protected long getRecordCount()
    {
        return recordCount;
    }

    public abstract void onInit( Map<String, String> properties ) throws WorkloadException;

    public final void cleanup() throws WorkloadException
    {
        if ( true == isCleanedUp )
        {
            throw new WorkloadException( "Workload may be cleaned up only once" );
        }
        isCleanedUp = true;
        onCleanup();
    }

    protected abstract void onCleanup() throws WorkloadException;

    public final Generator<Operation<?>> getLoadOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException
    {
        if ( -1 == getOperationCount() )
        {
            return createLoadOperations( generatorBuilder );
        }
        else
        {
            return new CappedGeneratorWrapper<Operation<?>>( createLoadOperations( generatorBuilder ),
                    getOperationCount() );
        }
    }

    protected abstract Generator<Operation<?>> createLoadOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException;

    public final Generator<Operation<?>> getTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException
    {
        if ( -1 == getOperationCount() )
        {
            return createTransactionalOperations( generatorBuilder );
        }
        else
        {
            return new CappedGeneratorWrapper<Operation<?>>( createTransactionalOperations( generatorBuilder ),
                    getOperationCount() );
        }
    }

    protected abstract Generator<Operation<?>> createTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException;
}
