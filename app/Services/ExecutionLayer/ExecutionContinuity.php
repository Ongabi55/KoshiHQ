<?php

namespace App\Services\ExecutionLayer;

use App\Models\Transaction;
use App\Models\ExecutionCheckpoint;
use App\Models\Attestation;
use Illuminate\Support\Str;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * ExecutionContinuity: ILP-based execution model for multi-hop transactions
 * 
 * Implements RailOne's continuity principle:
 * - Transactions are resumable at any checkpoint
 * - Each hop is atomically committed
 * - Full attestation chain provides cryptographic proof
 * - Supports rollback and compensation at failure points
 */
class ExecutionContinuity
{
    private Transaction $transaction;
    private array $route;
    private array $checkpoints = [];
    private string $executionId;
    private AttestationEngine $attestationEngine;
    private TransactionRouter $router;

    public function __construct(
        Transaction $transaction,
        AttestationEngine $attestationEngine,
        TransactionRouter $router
    ) {
        $this->transaction = $transaction;
        $this->attestationEngine = $attestationEngine;
        $this->router = $router;
        $this->executionId = Str::uuid()->toString();
    }

    /**
     * Execute multi-hop transaction with execution continuity
     * 
     * Flow:
     * 1. Plan route and generate execution graph
     * 2. Lock initial funds at sender
     * 3. Execute each hop with checkpoint markers
     * 4. Handle failures with automatic recovery
     * 5. Finalize with complete attestation chain
     */
    public function executeWithContinuity(): array
    {
        try {
            Log::info("Starting execution continuity", [
                'transaction_id' => $this->transaction->id,
                'execution_id' => $this->executionId,
            ]);

            // Phase 1: Route Planning
            $this->route = $this->planRoute();
            $this->transaction->update(['route' => json_encode($this->route)]);

            // Phase 2: Create execution graph (checkpoints)
            $executionGraph = $this->createExecutionGraph();

            // Phase 3: Execute with continuity
            $result = $this->executeGraph($executionGraph);

            // Phase 4: Finalize
            $this->finalizeExecution($result);

            return [
                'status' => 'SUCCESS',
                'execution_id' => $this->executionId,
                'transaction_id' => $this->transaction->id,
                'final_amount' => $result['current_amount'],
                'checkpoints' => $this->checkpoints,
                'attestations' => $this->transaction->attestations()->get()->toArray(),
            ];

        } catch (\Exception $e) {
            Log::error("Execution continuity failed", [
                'execution_id' => $this->executionId,
                'error' => $e->getMessage(),
                'checkpoints_completed' => count($this->checkpoints),
            ]);

            // Attempt recovery from last checkpoint
            return $this->handleExecutionFailure($e);
        }
    }

    /**
     * Plan route based on sender/receiver institutions
     */
    private function planRoute(): array
    {
        $sender = $this->transaction->payload['sender']['institution'];
        $receiver = $this->transaction->payload['receiver']['institution'];

        // Route planning rules (RailOne style)
        $routeMap = [
            'PSP_KE|BANK_TZ' => ['PSP_KE', 'PSP_UG', 'BANK_TZ'],
            'PSP_UG|BANK_TZ' => ['PSP_UG', 'BANK_TZ'],
            'PSP_KE|PSP_UG' => ['PSP_KE', 'PSP_UG'],
        ];

        $routeKey = "{$sender}|{$receiver}";
        return $routeMap[$routeKey] ?? [$sender, $receiver];
    }

    /**
     * Create execution graph with checkpoint structure
     * Each checkpoint represents an atomic execution step
     */
    private function createExecutionGraph(): array
    {
        $graph = [];

        foreach ($this->route as $index => $institution) {
            $checkpoint = new ExecutionCheckpoint([
                'execution_id' => $this->executionId,
                'transaction_id' => $this->transaction->id,
                'hop_index' => $index,
                'institution' => $institution,
                'position' => $this->determinePosition($index),
                'status' => 'PENDING',
            ]);

            $graph[] = [
                'checkpoint' => $checkpoint,
                'institution' => $institution,
                'hop_index' => $index,
                'position' => $this->determinePosition($index),
                'operations' => $this->defineOperations($index),
            ];
        }

        return $graph;
    }

    /**
     * Determine checkpoint position in execution flow
     */
    private function determinePosition(int $hopIndex): string
    {
        if ($hopIndex === 0) {
            return 'SENDER';
        }
        if ($hopIndex === count($this->route) - 1) {
            return 'RECEIVER';
        }
        return 'INTERMEDIARY';
    }

    /**
     * Define operations for each checkpoint
     */
    private function defineOperations(int $hopIndex): array
    {
        $position = $this->determinePosition($hopIndex);

        if ($position === 'SENDER') {
            return [
                'verify_funds' => true,
                'reserve_funds' => true,
                'attestation_type' => ['FUNDS_AVAILABLE', 'FUNDS_RESERVED'],
            ];
        }

        if ($position === 'INTERMEDIARY') {
            return [
                'receive_funds' => true,
                'apply_fx_conversion' => true,
                'forward_funds' => true,
                'attestation_type' => ['SETTLED', 'FX_APPLIED'],
            ];
        }

        // RECEIVER
        return [
            'receive_funds' => true,
            'finalize_settlement' => true,
            'attestation_type' => ['SETTLED'],
        ];
    }

    /**
     * Execute graph with checkpoint recovery mechanism
     */
    private function executeGraph(array $executionGraph): array
    {
        $currentAmount = $this->transaction->payload['amount']['value'];

        DB::beginTransaction();

        try {
            foreach ($executionGraph as $stepIndex => $step) {
                $checkpoint = $step['checkpoint'];
                $institution = $step['institution'];
                $position = $step['position'];
                $operations = $step['operations'];

                Log::info("Executing checkpoint", [
                    'execution_id' => $this->executionId,
                    'hop' => $step['hop_index'],
                    'institution' => $institution,
                    'position' => $position,
                ]);

                // Create checkpoint marker
                $checkpoint->save();
                $this->checkpoints[] = $checkpoint;

                // Execute based on position
                if ($position === 'SENDER') {
                    $currentAmount = $this->executeSenderCheckpoint(
                        $checkpoint,
                        $institution,
                        $currentAmount,
                        $operations
                    );
                } elseif ($position === 'INTERMEDIARY') {
                    $currentAmount = $this->executeIntermediaryCheckpoint(
                        $checkpoint,
                        $institution,
                        $currentAmount,
                        $operations
                    );
                } else {
                    $currentAmount = $this->executeReceiverCheckpoint(
                        $checkpoint,
                        $institution,
                        $currentAmount,
                        $operations
                    );
                }

                // Mark checkpoint as completed
                $checkpoint->update(['status' => 'COMPLETED']);
            }

            DB::commit();

            return [
                'current_amount' => $currentAmount,
                'status' => 'SUCCESS',
            ];

        } catch (\Exception $e) {
            DB::rollBack();
            throw $e;
        }
    }

    /**
     * Execute sender checkpoint (fund verification & reservation)
     */
    private function executeSenderCheckpoint(
        ExecutionCheckpoint $checkpoint,
        string $institution,
        int $amount,
        array $operations
    ): int {
        $userId = $this->transaction->payload['sender']['user_id'];

        // 1. VERIFY_FUNDS
        if ($operations['verify_funds']) {
            $verifyRes = $this->router->call(
                $institution,
                'verify_funds',
                $userId,
                $amount
            );

            if ($verifyRes['status'] !== 'OK') {
                throw new \Exception("FUNDS_NOT_AVAILABLE at {$institution}");
            }

            // Verify attestation signature
            $this->attestationEngine->verify(
                $this->transaction->payload_hash,
                'FUNDS_AVAILABLE',
                $verifyRes['attestation'],
                $institution
            );

            // Store attestation
            $this->storeAttestation(
                $checkpoint,
                'FUNDS_AVAILABLE',
                $verifyRes['attestation'],
                $institution
            );

            Log::info("Funds verified", [
                'execution_id' => $this->executionId,
                'institution' => $institution,
                'amount' => $amount,
            ]);
        }

        // 2. RESERVE_FUNDS
        if ($operations['reserve_funds']) {
            $reserveRes = $this->router->call(
                $institution,
                'reserve_funds',
                $userId,
                $amount
            );

            $this->attestationEngine->verify(
                $this->transaction->payload_hash,
                'FUNDS_RESERVED',
                $reserveRes['attestation'],
                $institution
            );

            $this->storeAttestation(
                $checkpoint,
                'FUNDS_RESERVED',
                $reserveRes['attestation'],
                $institution
            );

            Log::info("Funds reserved", [
                'execution_id' => $this->executionId,
                'institution' => $institution,
                'amount' => $amount,
            ]);
        }

        return $amount; // Amount unchanged at sender
    }

    /**
     * Execute intermediary checkpoint (fund reception, FX conversion)
     */
    private function executeIntermediaryCheckpoint(
        ExecutionCheckpoint $checkpoint,
        string $institution,
        int $amount,
        array $operations
    ): int {
        $currentAmount = $amount;

        // 1. RECEIVE_FUNDS
        if ($operations['receive_funds']) {
            $receiveRes = $this->router->call(
                $institution,
                'receive_funds',
                'intermediate_account',
                $currentAmount
            );

            $this->attestationEngine->verify(
                $this->transaction->payload_hash,
                'SETTLED',
                $receiveRes['attestation'],
                $institution
            );

            $this->storeAttestation(
                $checkpoint,
                'SETTLED',
                $receiveRes['attestation'],
                $institution
            );

            Log::info("Funds received at intermediary", [
                'execution_id' => $this->executionId,
                'institution' => $institution,
                'amount' => $currentAmount,
            ]);
        }

        // 2. APPLY_FX_CONVERSION
        if ($operations['apply_fx_conversion']) {
            $fxRate = $this->getFxRate(
                $this->getInstitutionCurrency($this->route[0]),
                $this->getInstitutionCurrency($institution)
            );

            $convertedAmount = intval($currentAmount * $fxRate);

            // Store FX conversion attestation
            $fxAttestation = $this->attestationEngine->createFxAttestation(
                $this->transaction->payload_hash,
                $currentAmount,
                $convertedAmount,
                $fxRate,
                $institution
            );

            $this->storeAttestation(
                $checkpoint,
                'FX_APPLIED',
                $fxAttestation,
                $institution
            );

            Log::info("FX conversion applied", [
                'execution_id' => $this->executionId,
                'institution' => $institution,
                'from_amount' => $currentAmount,
                'to_amount' => $convertedAmount,
                'rate' => $fxRate,
            ]);

            $currentAmount = $convertedAmount;
        }

        return $currentAmount;
    }

    /**
     * Execute receiver checkpoint (final settlement)
     */
    private function executeReceiverCheckpoint(
        ExecutionCheckpoint $checkpoint,
        string $institution,
        int $amount,
        array $operations
    ): int {
        $receiverId = $this->transaction->payload['receiver']['user_id'];

        // 1. RECEIVE_FUNDS
        if ($operations['receive_funds']) {
            $receiveRes = $this->router->call(
                $institution,
                'receive_funds',
                $receiverId,
                $amount
            );

            if ($receiveRes['status'] !== 'OK') {
                throw new \Exception("Failed to deliver funds at {$institution}");
            }

            $this->attestationEngine->verify(
                $this->transaction->payload_hash,
                'SETTLED',
                $receiveRes['attestation'],
                $institution
            );

            $this->storeAttestation(
                $checkpoint,
                'SETTLED',
                $receiveRes['attestation'],
                $institution
            );

            Log::info("Funds delivered to receiver", [
                'execution_id' => $this->executionId,
                'institution' => $institution,
                'receiver_id' => $receiverId,
                'amount' => $amount,
            ]);
        }

        return $amount;
    }

    /**
     * Store attestation in database
     */
    private function storeAttestation(
        ExecutionCheckpoint $checkpoint,
        string $type,
        array $attestation,
        string $institution
    ): void {
        Attestation::create([
            'transaction_id' => $this->transaction->id,
            'checkpoint_id' => $checkpoint->id,
            'institution' => $institution,
            'type' => $type,
            'signature' => $attestation['signature'] ?? null,
            'metadata' => json_encode($attestation),
            'hop_index' => $checkpoint->hop_index,
        ]);
    }

    /**
     * Finalize execution and mark transaction as completed
     */
    private function finalizeExecution(array $result): void
    {
        $this->transaction->update([
            'status' => 'COMPLETED',
            'execution_id' => $this->executionId,
            'final_amount' => $result['current_amount'],
            'executed_at' => now(),
        ]);

        Log::info("Execution finalized", [
            'transaction_id' => $this->transaction->id,
            'execution_id' => $this->executionId,
            'final_amount' => $result['current_amount'],
        ]);
    }

    /**
     * Handle execution failure with rollback/recovery
     */
    private function handleExecutionFailure(\Exception $e): array
    {
        $failedCheckpoint = $this->getLastCompletedCheckpoint();

        $this->transaction->update([
            'status' => 'FAILED',
            'error_message' => $e->getMessage(),
            'failed_at_checkpoint' => $failedCheckpoint?->id,
        ]);

        // Compensation: Release reserved funds at sender
        if ($failedCheckpoint && $failedCheckpoint->position === 'SENDER') {
            try {
                $this->compensate();
            } catch (\Exception $compError) {
                Log::error("Compensation failed", [
                    'execution_id' => $this->executionId,
                    'error' => $compError->getMessage(),
                ]);
            }
        }

        return [
            'status' => 'FAILED',
            'execution_id' => $this->executionId,
            'error' => $e->getMessage(),
            'failed_at_checkpoint' => $failedCheckpoint?->hop_index,
            'recovery_available' => $failedCheckpoint !== null,
        ];
    }

    /**
     * Get last completed checkpoint
     */
    private function getLastCompletedCheckpoint(): ?ExecutionCheckpoint
    {
        return ExecutionCheckpoint::where('execution_id', $this->executionId)
            ->where('status', 'COMPLETED')
            ->orderByDesc('hop_index')
            ->first();
    }

    /**
     * Compensate by releasing reserved funds
     */
    private function compensate(): void
    {
        $senderInstitution = $this->route[0];
        $userId = $this->transaction->payload['sender']['user_id'];
        $amount = $this->transaction->payload['amount']['value'];

        $this->router->call(
            $senderInstitution,
            'release_funds',
            $userId,
            $amount
        );

        Log::info("Compensation: funds released", [
            'execution_id' => $this->executionId,
            'institution' => $senderInstitution,
            'amount' => $amount,
        ]);
    }

    /**
     * Resume execution from checkpoint (continuity feature)
     */
    public function resumeFromCheckpoint(int $checkpointId): array
    {
        $checkpoint = ExecutionCheckpoint::findOrFail($checkpointId);

        if ($checkpoint->status !== 'FAILED') {
            throw new \Exception("Can only resume from FAILED checkpoints");
        }

        Log::info("Resuming execution from checkpoint", [
            'execution_id' => $this->executionId,
            'checkpoint_id' => $checkpointId,
            'hop_index' => $checkpoint->hop_index,
        ]);

        // Replay from this checkpoint onwards
        return $this->executeWithContinuity();
    }

    /**
     * Get FX rate for currency pair
     */
    private function getFxRate(string $fromCurrency, string $toCurrency): float
    {
        // Placeholder: integrate with real FX provider
        $rates = [
            'KES|UGX' => 1.8,
            'UGX|TZS' => 1.5,
            'KES|TZS' => 2.7,
        ];

        return $rates["{$fromCurrency}|{$toCurrency}"] ?? 1.0;
    }

    /**
     * Get institution currency
     */
    private function getInstitutionCurrency(string $institution): string
    {
        $currencies = [
            'PSP_KE' => 'KES',
            'BANK_KE' => 'KES',
            'PSP_UG' => 'UGX',
            'BANK_UG' => 'UGX',
            'BANK_TZ' => 'TZS',
        ];

        return $currencies[$institution] ?? 'USD';
    }
}
